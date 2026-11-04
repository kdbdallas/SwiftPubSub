import Foundation
import Observation

struct PubSubScope: Equatable, Sendable {
    let clientID: String
    let sessionID: String?

    func webSocketURL(serverURL: URL) -> URL {
        let base = serverURL
            .appending(path: "ws")
            .appending(path: "clients")
            .appending(path: clientID)
        guard let sessionID else { return base }
        return base.appending(path: "sessions").appending(path: sessionID)
    }
}

struct PubSubMessage: Codable, Equatable, Identifiable, Sendable {
    struct Scope: Codable, Equatable, Sendable {
        let clientId: String
        let sessionId: String?
    }

    let type: String
    let id: UUID
    let scope: Scope
    let message: String
    let sentAt: Date
}

@MainActor
@Observable
final class PubSubViewModel {
    private(set) var messages: [PubSubMessage] = []
    private(set) var isConnected = false
    private(set) var errorMessage: String?

    private var socket: URLSessionWebSocketTask?
    private var receiveTask: Task<Void, Never>?

    func connect(serverURL: URL, scope: PubSubScope) {
        disconnect()
        let task = URLSession.shared.webSocketTask(with: scope.webSocketURL(serverURL: serverURL))
        socket = task
        task.resume()
        isConnected = true
        receiveTask = Task { [weak self] in
            await self?.receiveMessages(from: task)
        }
    }

    func send(_ text: String) {
        guard !text.isEmpty, let socket else { return }
        let payload = ["type": "publish", "message": text]
        do {
            let data = try JSONSerialization.data(withJSONObject: payload)
            socket.send(.data(data)) { [weak self] error in
                guard let error else { return }
                Task { @MainActor in self?.errorMessage = error.localizedDescription }
            }
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    func disconnect() {
        receiveTask?.cancel()
        receiveTask = nil
        socket?.cancel(with: .goingAway, reason: nil)
        socket = nil
        isConnected = false
    }

    private func receiveMessages(from socket: URLSessionWebSocketTask) async {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        do {
            while !Task.isCancelled {
                let frame = try await socket.receive()
                let data: Data
                switch frame {
                case .data(let value): data = value
                case .string(let value): data = Data(value.utf8)
                @unknown default: continue
                }
                let eventType = try JSONSerialization.jsonObject(with: data) as? [String: Any]
                guard eventType?["type"] as? String == "message" else { continue }
                let decoded = try decoder.decode(PubSubMessage.self, from: data)
                messages.insert(decoded, at: 0)
            }
        } catch {
            guard !Task.isCancelled else { return }
            errorMessage = error.localizedDescription
            isConnected = false
        }
    }
}
