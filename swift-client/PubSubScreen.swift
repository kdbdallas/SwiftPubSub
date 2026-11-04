import SwiftUI

struct PubSubScreen: View {
    @State private var model = PubSubViewModel()
    @State private var draft = ""

    let serverURL: URL
    let scope: PubSubScope

    var body: some View {
        VStack(spacing: 12) {
            List(model.messages) { item in
                VStack(alignment: .leading) {
                    Text(item.message)
                    Text(item.sentAt, format: .dateTime.hour().minute().second())
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }

            HStack {
                TextField("Message", text: $draft)
                Button("Send") {
                    model.send(draft)
                    draft = ""
                }
                .disabled(draft.isEmpty || !model.isConnected)
            }
            if let errorMessage = model.errorMessage {
                Text(errorMessage).foregroundStyle(.red)
            }
        }
        .padding()
        .navigationTitle("Client \(scope.clientID)")
        .task { model.connect(serverURL: serverURL, scope: scope) }
        .onDisappear { model.disconnect() }
    }
}
