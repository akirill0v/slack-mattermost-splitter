use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Chunk {
    pub items: Vec<ChunkItem>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct ChunkItem {
    pub id: String,
    pub files: Vec<(String, usize)>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Direct {
    pub id: String,
    pub members: Vec<String>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct SlackPost {
    #[serde(flatten)]
    pub extra: serde_json::Value,

    #[serde(default)]
    pub upload: bool,
    // Legacy...
    pub file: Option<File>,
    // May be empty
    #[serde(default)]
    pub files: Vec<File>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct File {
    #[serde(flatten)]
    pub extra: serde_json::Value,

    // Add fields for the File struct here
    #[serde(default)]
    pub is_external: bool,
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub url_private: String,
    #[serde(default)]
    pub url_private_download: String,
}

impl File {
    pub fn url_for_download(&self) -> String {
        vec![self.url_private_download.clone(), self.url_private.clone()]
            .into_iter()
            .find(|u| !u.is_empty())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_slack_post_serialization() {
        let file = File {
            extra: json!({"id": "F12345", "name": "example.txt"}),
            ..Default::default()
        };
        let slack_post = SlackPost {
            extra: json!({"channel": "C12345", "user": "U12345"}),
            file: Some(file.clone()),
            files: vec![file],
            ..Default::default()
        };

        let serialized = serde_json::to_string(&slack_post).unwrap();
        let deserialized: SlackPost = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.extra, slack_post.extra);
        assert_eq!(
            deserialized.file.unwrap().extra,
            slack_post.file.unwrap().extra
        );
        assert_eq!(deserialized.files.len(), slack_post.files.len());
        assert_eq!(deserialized.files[0].extra, slack_post.files[0].extra);
    }

    #[test]
    fn test_file_serialization() {
        let file = File {
            extra: json!({"id": "F12345", "name": "example.txt", "size": 1024}),
            ..Default::default()
        };

        let serialized = serde_json::to_string(&file).unwrap();
        let deserialized: File = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.extra, file.extra);
    }
}
