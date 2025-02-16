pub struct TcpTransportConfig {
    pub server_address: String,
}

impl Default for TcpTransportConfig {
    fn default() -> Self {
        TcpTransportConfig {
            server_address: "127.0.0.1:0".to_string()
        }
    }
}
