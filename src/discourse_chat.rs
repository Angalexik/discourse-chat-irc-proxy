use color_eyre::eyre::{Context, OptionExt, Result, eyre};
use core::task;
use futures::{
    FutureExt, Stream, StreamExt,
    future::LocalBoxFuture,
    io,
    stream::{self, LocalBoxStream},
};
use pin_project::pin_project;
#[allow(unused_imports)]
use reqwest::Proxy;
use reqwest::{Client, IntoUrl, Url, header::HeaderMap};
use serde::{Deserialize, Serialize};
use std::pin::pin;
use std::{
    collections::HashMap,
    pin::Pin,
    task::{Poll, ready},
};
use strum::{EnumString, IntoStaticStr};
use time::{
    UtcDateTime,
    format_description::well_known::{Iso8601, Rfc3339},
};
use tokio_util::io::StreamReader;
use tokio_util::{
    bytes::Bytes,
    codec::{Decoder, FramedRead},
};
use uuid::Uuid;

use crate::ISO8601_CONFIG;

#[derive(Deserialize, Debug, Hash, PartialEq, Eq, Clone)]
pub struct DiscourseUser {
    pub username: String,
    pub id: i64,
}

#[derive(Deserialize, Debug)]
struct DiscourseRepliedToMessage {
    id: i64,
}
#[derive(Deserialize, Debug)]
struct DiscourseMessage {
    message: String,
    user: DiscourseUser,
    id: i64,
    created_at: String,
    in_reply_to: Option<DiscourseRepliedToMessage>,
}

#[derive(Deserialize, Debug, EnumString, IntoStaticStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum AddOrRemove {
    Add,
    Remove,
}

#[derive(Deserialize, Debug)]
struct DiscourseReact {
    action: AddOrRemove,
    user: DiscourseUser,
    emoji: String,
    chat_message_id: i64,
}

pub enum MessageBusChat {
    Message(ChatMessage),
    Reaction {
        sender: String,
        reaction_to: i64,
        action: AddOrRemove,
        emoji_name: String,
    },
}

#[allow(dead_code)]
#[derive(Deserialize, Serialize, Clone)]
pub struct MessageBusMessage {
    global_id: i64,
    message_id: i64,
    pub channel: String,
    data: serde_json::Value,
}

impl MessageBusMessage {
    pub fn deserialize_chat(self) -> Result<MessageBusChat> {
        #[derive(Deserialize, Debug)]
        #[serde(tag = "type")]
        #[serde(rename_all = "lowercase")]
        enum Data {
            Sent { chat_message: DiscourseMessage },
            Reaction(DiscourseReact),
        }

        let deserialized = serde_json::from_value::<Data>(self.data)?;

        Ok(match deserialized {
            Data::Sent { chat_message } => MessageBusChat::Message(chat_message.into()),
            Data::Reaction(reaction) => MessageBusChat::Reaction {
                sender: reaction.user.username,
                reaction_to: reaction.chat_message_id,
                action: reaction.action,
                emoji_name: reaction.emoji,
            },
        })
    }

    pub fn deserialize_presence(self) -> (Vec<DiscourseUser>, Vec<i64>) {
        #[derive(Deserialize)]
        struct Data {
            entering_users: Option<Vec<DiscourseUser>>,
            leaving_user_ids: Option<Vec<i64>>,
        }

        let deserialized = serde_json::from_value::<Data>(self.data).unwrap();

        (
            deserialized.entering_users.unwrap_or_default(),
            deserialized.leaving_user_ids.unwrap_or_default(),
        )
    }
}

struct MessageBusCodec {}

impl MessageBusCodec {
    const DELIMITER: &[u8] = "\r\n|\r\n".as_bytes();

    fn parse_message(
        &self,
        bytes: Bytes,
    ) -> std::result::Result<<Self as Decoder>::Item, <Self as Decoder>::Error> {
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    }
}

impl Decoder for MessageBusCodec {
    type Item = Vec<MessageBusMessage>;
    type Error = io::Error;

    fn decode(
        &mut self,
        buf: &mut tokio_util::bytes::BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        let offset = buf.windows(5).position(|bytes| bytes == Self::DELIMITER);

        if let Some(offset) = offset {
            let mut chunk = buf.split_to(offset + Self::DELIMITER.len());
            chunk.truncate(chunk.len() - Self::DELIMITER.len());
            let chunk = chunk.freeze();
            let messages = self.parse_message(chunk);

            Ok(Some(messages?))
        } else {
            Ok(None)
        }
    }
}

type MessageBusStreamItem =
    std::result::Result<MessageBusMessage, <MessageBusCodec as Decoder>::Error>;

#[pin_project(project = MessageBusStateProj)]
enum MessageBusState<'a> {
    Connected(LocalBoxStream<'a, MessageBusStreamItem>),
    Connecting(LocalBoxFuture<'a, Result<LocalBoxStream<'a, MessageBusStreamItem>>>),
}

#[derive(Clone)]
struct MessageBusInner {
    client: Client,
    headers: HeaderMap,
    base_url: Url,
    channels: HashMap<String, i64>,
    client_id: u128,
    sequence_number: i64,
}

impl MessageBusInner {
    async fn get_stream<'a>(mut self) -> Result<LocalBoxStream<'a, MessageBusStreamItem>> {
        self.channels
            .insert("__seq".to_string(), self.sequence_number);
        let response = self
            .client
            .post(
                self.base_url
                    .join("/message-bus/")?
                    .join(&format!("{:#x}/", self.client_id))?
                    .join("poll")?,
            )
            .form(&self.channels)
            .headers(self.headers)
            .send()
            .await?;

        let messages = FramedRead::new(
            StreamReader::new(response.bytes_stream().map(|r| r.map_err(io::Error::other))),
            MessageBusCodec {},
        )
        .flat_map(|item| {
            let msgs: Vec<_> = match item {
                Ok(inner) => inner.iter().map(|x| Ok(x.clone())).collect(),
                Err(e) => vec![Err(e)],
            };

            stream::iter(msgs)
        });

        Ok(messages.boxed_local())
    }
}

#[pin_project]
pub struct MessageBus<'a> {
    inner: MessageBusInner,
    #[pin]
    state: MessageBusState<'a>,
}

impl MessageBus<'_> {
    pub fn new(chat_client: DiscourseChatClient, channels: &[&str]) -> Self {
        let client = chat_client.client;
        let headers = chat_client.headers;
        let base_url = chat_client.base_url;
        let client_id: u128 = rand::random();
        let channels = channels
            .iter()
            .map(|channel| (channel.to_string(), -1))
            .collect();

        let inner = MessageBusInner {
            client,
            headers,
            base_url,
            channels,
            client_id,
            sequence_number: 1,
        };
        let future = inner.clone().get_stream().boxed_local();

        Self {
            inner,
            state: MessageBusState::Connecting(future),
        }
    }
}

impl Stream for MessageBus<'_> {
    type Item = Result<MessageBusMessage>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        match this.state.as_mut().project() {
            MessageBusStateProj::Connected(stream) => {
                if let Some(message) = ready!(stream.as_mut().poll_next(cx)) {
                    // TODO: Handle __status properly
                    if let Ok(MessageBusMessage {
                        message_id,
                        channel,
                        ..
                    }) = &message
                        && let Some(entry) = this.inner.channels.get_mut(channel)
                    {
                        *entry = *message_id;
                    }

                    match message {
                        Err(e) => {
                            let error = e.downcast::<reqwest::Error>();
                            if let Ok(ref e) = error
                                && e.is_decode()
                            {
                                eprintln!("error decoding body!, retrying messagebus request");
                            } else {
                                let e: color_eyre::Report = match error {
                                    Ok(e) => e.into(),
                                    Err(e) => e.into(),
                                };
                                return Poll::Ready(Some(
                                    Err(e).wrap_err("Getting MessageBus message"),
                                ));
                            }
                        }
                        Ok(m) => return Poll::Ready(Some(Ok(m))),
                    }
                }

                this.inner.sequence_number += 1;
                let future = this.inner.clone().get_stream().boxed_local();

                this.state.set(MessageBusState::Connecting(future));

                self.poll_next(cx)
            }
            MessageBusStateProj::Connecting(future) => match ready!(future.as_mut().poll(cx)) {
                Ok(stream) => {
                    self.state = MessageBusState::Connected(stream);
                    self.poll_next(cx)
                }
                Err(e) => Poll::Ready(Some(Err(e))),
            },
        }
    }
}

pub struct ChatMessage {
    pub text: String,
    pub sender: String,
    pub timestamp: UtcDateTime,
    pub id: i64,
    pub replying_to: Option<i64>,
}

impl From<&DiscourseMessage> for ChatMessage {
    fn from(value: &DiscourseMessage) -> Self {
        ChatMessage {
            text: value.message.clone(),
            sender: value.user.username.clone(),
            id: value.id,
            timestamp: UtcDateTime::parse(&value.created_at, &Rfc3339).unwrap(),
            replying_to: value.in_reply_to.as_ref().map(|m| m.id),
        }
    }
}

impl From<DiscourseMessage> for ChatMessage {
    fn from(value: DiscourseMessage) -> Self {
        From::from(&value)
    }
}

#[derive(Clone)]
pub struct DiscourseChatClient {
    client: Client,
    headers: HeaderMap,
    base_url: Url,
    pub username: String,
}

pub trait ChatClient {
    fn get_username(&self) -> &str;

    async fn message_backlog(&self) -> Result<Vec<ChatMessage>>;
    async fn send_message(&self, text: &str, replying_to: Option<i64>) -> Result<i64>;
    async fn send_react(&self, emoji_name: &str, replying_to: i64) -> Result<()>;
    async fn send_unreact(&self, emoji_name: &str, replying_to: i64) -> Result<()>;
    async fn list_users(&self) -> Result<Vec<DiscourseUser>>;
}

impl DiscourseChatClient {
    pub async fn new(base_url: impl IntoUrl, login: String, password: &str) -> Result<Self> {
        let base_url = base_url.into_url()?;

        let client = Client::builder()
            .cookie_store(true)
            .user_agent("DiscourseIRC/0.0.1")
            // .proxy(Proxy::all("http://127.0.0.1:8080")?)
            .build()?;
        let csrf_token = client
            .get(base_url.join("/session/csrf.json")?)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?["csrf"]
            .as_str()
            .ok_or_eyre("CSRF token was not a string")?
            .to_owned();

        let mut headers = HeaderMap::new();
        headers.insert("X-CSRF-Token", csrf_token.parse()?);
        headers.insert("X-Requested-With", "XMLHttpRequest".parse()?);

        client
            .post(base_url.join("/session.json")?)
            .headers(headers.clone())
            .form(&HashMap::from([
                ("login", login.as_str()),
                ("password", password),
            ]))
            .send()
            .await?
            .error_for_status()?;

        Ok(Self {
            client,
            headers,
            base_url,
            username: login,
        })
    }

    async fn send_react_common(
        &self,
        emoji_name: &str,
        replying_to: i64,
        add_or_remove: AddOrRemove,
    ) -> Result<()> {
        #[derive(Deserialize)]
        struct ApiResponse {
            success: String,
        }

        let response = self
            .client
            .put(
                self.base_url
                    .join(&format!("/chat/4/react/{replying_to}"))?,
            )
            .headers(self.headers.clone())
            .form(&[
                ("emoji", emoji_name),
                ("react_action", add_or_remove.into()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json::<ApiResponse>()
            .await?;

        let status = response.success;
        if status == "OK" {
            Ok(())
        } else {
            Err(eyre!("expected status `OK`, instead received `{status}`"))
        }
    }
}

impl ChatClient for DiscourseChatClient {
    async fn message_backlog(&self) -> Result<Vec<ChatMessage>> {
        #[derive(Deserialize)]
        struct ApiResponse {
            messages: Vec<DiscourseMessage>,
        }

        let messages = self
            .client
            .get(self.base_url.join("/chat/api/channels/4/messages")?)
            .headers(self.headers.clone())
            .query(&[("page_size", 50)])
            .send()
            .await?
            .json::<ApiResponse>()
            .await?
            .messages;

        Ok(messages.iter().map(|m| m.into()).collect())
    }

    async fn send_message(&self, text: &str, replying_to: Option<i64>) -> Result<i64> {
        #[derive(Deserialize)]
        struct ApiResponse {
            success: String,
            message_id: i64,
        }

        let timestamp = UtcDateTime::now().format(&Iso8601::<ISO8601_CONFIG>)?;
        let response = self
            .client
            .post(self.base_url.join("/chat/4")?)
            .headers(self.headers.clone())
            .form(&[
                ("message", Some(text)),
                ("staged_id", Some(&Uuid::new_v4().to_string())),
                ("client_created_at", Some(&timestamp)),
                (
                    "in_reply_to_id",
                    replying_to.map(|i| i.to_string()).as_deref(),
                ),
            ])
            .send()
            .await?
            .error_for_status()?
            .json::<ApiResponse>()
            .await?;

        let status = response.success;
        if status == "OK" {
            Ok(response.message_id)
        } else {
            Err(eyre!("expected status `OK`, instead received `{status}`"))
        }
    }

    async fn send_react(&self, emoji_name: &str, replying_to: i64) -> Result<()> {
        self.send_react_common(emoji_name, replying_to, AddOrRemove::Add)
            .await
    }

    async fn send_unreact(&self, emoji_name: &str, replying_to: i64) -> Result<()> {
        self.send_react_common(emoji_name, replying_to, AddOrRemove::Remove)
            .await
    }

    async fn list_users(&self) -> Result<Vec<DiscourseUser>> {
        #[derive(Deserialize)]
        struct GlobalPresenceChannelState {
            users: Vec<DiscourseUser>,
        }

        #[derive(Deserialize)]
        struct ApiResponse {
            global_presence_channel_state: GlobalPresenceChannelState,
        }

        Ok(self
            .client
            .get(self.base_url.join("/chat/api/me/channels")?)
            .headers(self.headers.clone())
            .send()
            .await?
            .error_for_status()?
            .json::<ApiResponse>()
            .await?
            .global_presence_channel_state
            .users
            .into_iter()
            .collect())
    }

    fn get_username(&self) -> &str {
        &self.username
    }
}

#[cfg(test)]
mod tests {
    use crate::MessageBusMessage;

    use super::{ChatClient, DiscourseChatClient, MessageBus};

    use futures::StreamExt;
    use httpmock::{Mock, prelude::*};
    use reqwest::Url;
    use serde_json::json;

    struct MockLogin<'a> {
        mock_csrf: Mock<'a>,
        mock_session: Mock<'a>,
    }

    impl MockLogin<'_> {
        const CSRF_TOKEN: &'static str = "dummy_token";
        const SESSION_COOKIE: &'static str = "dummy_session";

        fn mock<'a>(server: &'a MockServer) -> MockLogin<'a> {
            let mock_csrf = server.mock(|when, then| {
                when.path("/session/csrf.json");
                then.status(200)
                    .json_body(json!({ "csrf": Self::CSRF_TOKEN }));
            });
            let mock_session = server.mock(|when, then| {
                when.method(POST)
                    .path("/session.json")
                    .header("X-CSRF-Token", Self::CSRF_TOKEN)
                    .form_urlencoded_tuple("login", "test_user")
                    .form_urlencoded_tuple("password", "test_password");
                then.status(200).header(
                    "Set-Cookie",
                    format!("_forum_session={}", Self::SESSION_COOKIE),
                );
            });

            MockLogin {
                mock_csrf,
                mock_session,
            }
        }
    }

    impl Drop for MockLogin<'_> {
        fn drop(&mut self) {
            self.mock_session.assert();
            self.mock_csrf.assert();
        }
    }

    #[tokio::test]
    async fn test_discourse_login() {
        let server = MockServer::start();
        // Assigned to `_mock` instead of just `_` in order for the value to get dropped at
        // the end of this function scope
        let _mock = MockLogin::mock(&server);

        let url: Url = server.base_url().as_str().try_into().unwrap();

        DiscourseChatClient::new(url.clone(), "test_user".to_string(), "test_password")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_message_bus() {
        let server = MockServer::start();
        let _mock = MockLogin::mock(&server);
        let message_bus_mock = server.mock(|when, then| {
            when.method(POST)
                .path_matches(r"/message-bus/[^/]+/poll")
                .header("X-CSRF-Token", MockLogin::CSRF_TOKEN)
                .cookie("_forum_session", MockLogin::SESSION_COOKIE)
                .form_urlencoded_tuple_exists("__seq")
                .form_urlencoded_tuple("/refresh_client", "-1");
            then.status(200).body(format!(
                "{}\r\n|\r\n",
                serde_json::to_string(&vec![MessageBusMessage {
                    global_id: -1,
                    message_id: -1,
                    channel: "/__status".to_string(),
                    data: json!({}),
                }])
                .unwrap()
            ));
        });

        let url: Url = server.base_url().as_str().try_into().unwrap();
        let chat_client = DiscourseChatClient::new(url, "test_user".to_string(), "test_password")
            .await
            .unwrap();
        let mut message_bus = MessageBus::new(chat_client, &["/refresh_client"]);

        assert_eq!(
            message_bus.next().await.unwrap().unwrap().channel,
            "/__status".to_string()
        );
        assert!(message_bus_mock.calls() > 0);
    }

    #[tokio::test]
    async fn test_new_message() {
        let server = MockServer::start();
        let _mock = MockLogin::mock(&server);

        let message_id = 2;

        let message_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/chat/4")
                .header("X-CSRF-Token", MockLogin::CSRF_TOKEN)
                .header("X-Requested-With", "XMLHttpRequest")
                .cookie("_forum_session", MockLogin::SESSION_COOKIE)
                .form_urlencoded_tuple("message", "test-message")
                .form_urlencoded_tuple_exists("staged_id")
                .form_urlencoded_tuple_exists("client_created_at")
                .form_urlencoded_tuple_missing("in_reply_to_id");
            then.status(200)
                .json_body(json!({ "success": "OK", "message_id": 2 }));
        });

        let url: Url = server.base_url().as_str().try_into().unwrap();
        let chat_client = DiscourseChatClient::new(url, "test_user".to_string(), "test_password")
            .await
            .unwrap();

        assert_eq!(
            chat_client
                .send_message("test-message", None)
                .await
                .unwrap(),
            message_id
        );
        message_mock.assert();
    }

    #[tokio::test]
    async fn test_reply() {
        let server = MockServer::start();
        let _mock = MockLogin::mock(&server);

        let message_id = 2;

        let message_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/chat/4")
                .header("X-CSRF-Token", MockLogin::CSRF_TOKEN)
                .header("X-Requested-With", "XMLHttpRequest")
                .cookie("_forum_session", MockLogin::SESSION_COOKIE)
                .form_urlencoded_tuple("message", "test-message")
                .form_urlencoded_tuple_exists("staged_id")
                .form_urlencoded_tuple_exists("client_created_at")
                .form_urlencoded_tuple("in_reply_to_id", "1");
            then.status(200)
                .json_body(json!({ "success": "OK", "message_id": 2 }));
        });

        let url: Url = server.base_url().as_str().try_into().unwrap();
        let chat_client = DiscourseChatClient::new(url, "test_user".to_string(), "test_password")
            .await
            .unwrap();

        assert_eq!(
            chat_client
                .send_message("test-message", Some(1))
                .await
                .unwrap(),
            message_id
        );
        message_mock.assert();
    }

    #[tokio::test]
    async fn test_react_unreact() {
        let server = MockServer::start();
        let _mock = MockLogin::mock(&server);

        let message_id = 2;
        let emoji = "distorted_face";

        let react_mock = server.mock(|when, then| {
            when.method(PUT)
                .path(format!("/chat/4/react/{message_id}"))
                .header("X-CSRF-Token", MockLogin::CSRF_TOKEN)
                .header("X-Requested-With", "XMLHttpRequest")
                .cookie("_forum_session", MockLogin::SESSION_COOKIE)
                .form_urlencoded_tuple("emoji", emoji)
                .form_urlencoded_tuple("react_action", "add");
            then.status(200).json_body(json!({ "success": "OK" }));
        });

        let unreact_mock = server.mock(|when, then| {
            when.method(PUT)
                .path(format!("/chat/4/react/{message_id}"))
                .header("X-CSRF-Token", MockLogin::CSRF_TOKEN)
                .header("X-Requested-With", "XMLHttpRequest")
                .cookie("_forum_session", MockLogin::SESSION_COOKIE)
                .form_urlencoded_tuple("emoji", emoji)
                .form_urlencoded_tuple("react_action", "remove");
            then.status(200).json_body(json!({ "success": "OK" }));
        });

        let url: Url = server.base_url().as_str().try_into().unwrap();
        let chat_client = DiscourseChatClient::new(url, "test_user".to_string(), "test_password")
            .await
            .unwrap();

        chat_client.send_react(emoji, message_id).await.unwrap();
        chat_client.send_unreact(emoji, message_id).await.unwrap();

        react_mock.assert();
        unreact_mock.assert();
    }
}
