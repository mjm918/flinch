#[cfg(test)]
mod tests {
    use flinch::schemas::Schemas;

    #[tokio::test]
    async fn test() {
        let schema = Schemas::init().await;
        assert!(schema.is_ok());

        let schema = schema.unwrap();

        let login = schema.login("root","flinch","*");
        assert!(login.is_ok(), "{}", login.err().unwrap());

        let session_id = login.unwrap();
        assert!(!session_id.is_empty());
    }
}