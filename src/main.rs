use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    routing::{delete, get, post},
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::ErrorKind,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, broadcast};
use tower_http::{cors::CorsLayer, services::ServeDir};
use uuid::Uuid;

// --- VERİ YAPILARI (DATA STRUCTURES) ---

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PostModel {
    id: u64,
    author: String,
    content: String,
    likes: u32,
    created_at: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct GroupModel {
    id: u64,
    name: String,
    members_count: u32,
    #[serde(default)]
    owner_email: Option<String>,
    #[serde(default)]
    moderator_emails: Vec<String>,
    #[serde(default)]
    member_emails: Vec<String>,
    #[serde(default)]
    categories: Vec<ChannelCategory>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ChannelCategory {
    id: String,
    name: String,
    channels: Vec<GroupChannel>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct GroupChannel {
    id: String,
    name: String,
    kind: String,
}

#[derive(Serialize)]
struct GroupView {
    id: u64,
    name: String,
    members_count: u32,
    owner: String,
    role: String,
    categories: Vec<ChannelCategory>,
}

#[derive(Serialize, Clone)]
struct GroupPresenceMember {
    nick: String,
    avatar_url: Option<String>,
    role: String,
    online: bool,
}

struct LiveGroupMember {
    connections: HashSet<String>,
}

struct GroupWebSocketTicket {
    email: String,
    expires_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct NotificationModel {
    id: u64,
    title: String,
    content: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct CreatePostPayload {
    author: String,
    content: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct CreateGroupPayload {
    name: String,
}

#[derive(Deserialize)]
struct AddGroupChannelPayload {
    category: String,
    name: String,
    kind: String,
}

#[derive(Deserialize)]
struct AssignGroupModeratorPayload {
    nick: String,
}

#[derive(Deserialize)]
struct YouTubeSearchQuery {
    q: String,
}

#[derive(Deserialize)]
struct YouTubeSearchResponse {
    items: Vec<YouTubeSearchItem>,
}

#[derive(Deserialize)]
struct YouTubeSearchItem {
    id: YouTubeSearchId,
    snippet: YouTubeSnippet,
}

#[derive(Deserialize)]
struct YouTubeSearchId {
    #[serde(rename = "videoId")]
    video_id: Option<String>,
}

#[derive(Deserialize)]
struct YouTubeSnippet {
    title: String,
    description: String,
    #[serde(rename = "channelTitle")]
    channel_title: String,
    thumbnails: YouTubeThumbnails,
}

#[derive(Deserialize)]
struct YouTubeThumbnails {
    high: Option<YouTubeThumbnail>,
    medium: Option<YouTubeThumbnail>,
    default: Option<YouTubeThumbnail>,
}

#[derive(Deserialize)]
struct YouTubeThumbnail {
    url: String,
}

#[derive(Serialize)]
struct GiphyConfig {
    api_key: Option<String>,
}

#[derive(Serialize)]
struct YouTubeVideo {
    id: String,
    title: String,
    description: String,
    channel_title: String,
    thumbnail_url: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct SavedVideo {
    provider: String,
    id: String,
    title: String,
    channel_title: String,
    thumbnail_url: String,
}

#[derive(Serialize, Deserialize, Default)]
struct UserVideoCollections {
    email: String,
    #[serde(default)]
    liked: Vec<SavedVideo>,
    #[serde(default)]
    favorites: Vec<SavedVideo>,
}

#[derive(Serialize)]
struct VideoCollectionsResponse {
    liked: Vec<SavedVideo>,
    favorites: Vec<SavedVideo>,
}

#[derive(Deserialize)]
struct UpdateVideoCollectionPayload {
    video: SavedVideo,
    saved: bool,
}

#[derive(Deserialize)]
struct YouTubeRecommendationQuery {
    topic: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct DirectMessage {
    id: String,
    from_email: String,
    to_email: String,
    from_nick: String,
    text: String,
    created_at: i64,
}

#[derive(Serialize)]
struct DirectMessageResponse {
    id: String,
    from_nick: String,
    text: String,
    created_at: i64,
}

#[derive(Deserialize)]
struct SendDirectMessagePayload {
    to_nick: String,
    text: String,
}

#[derive(Deserialize)]
struct DirectMessageQuery {
    nick: String,
}

#[derive(Serialize)]
struct TurnstileConfig {
    site_key: Option<String>,
    enabled: bool,
}

#[derive(Deserialize)]
struct TurnstilePayload {
    token: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct User {
    email: String,
    nick: String,
    password_hash: String,
    #[serde(default)]
    disabled: bool,
    #[serde(default)]
    avatar_url: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct FriendRequest {
    id: String,
    from_email: String,
    to_email: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct Friendship {
    user_a: String,
    user_b: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct Follow {
    follower_email: String,
    followed_email: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct StoryModel {
    id: String,
    author_email: String,
    author: String,
    text: String,
    #[serde(default)]
    image: Option<String>,
    #[serde(default)]
    video: Option<String>,
    created_at: i64,
    expires_at: i64,
}

#[derive(Serialize, Deserialize, Default)]
struct Database {
    #[serde(default)]
    users: Vec<User>,
    #[serde(default)]
    friend_requests: Vec<FriendRequest>,
    #[serde(default)]
    friendships: Vec<Friendship>,
    #[serde(default)]
    follows: Vec<Follow>,
    #[serde(default)]
    stories: Vec<StoryModel>,
    #[serde(default)]
    messages: Vec<serde_json::Value>,
    #[serde(default)]
    direct_messages: Vec<DirectMessage>,
    #[serde(default)]
    groups: Vec<GroupModel>,
    #[serde(default)]
    sessions: HashMap<String, Session>,
    #[serde(default)]
    video_collections: Vec<UserVideoCollections>,
}

#[derive(Deserialize)]
struct AuthPayload {
    nick: String,
    email: String,
    password: String,
    turnstile_token: String,
}

#[derive(Serialize)]
struct AuthResponse {
    session: String,
    nick: String,
    email: String,
    avatar_url: Option<String>,
}

#[derive(Deserialize)]
struct AvatarPayload {
    avatar_url: Option<String>,
}

#[derive(Deserialize)]
struct ChangePasswordPayload {
    current_password: String,
    new_password: String,
}

#[derive(Deserialize)]
struct NicknamePayload {
    nick: String,
}

#[derive(Serialize)]
struct FriendListResponse {
    friends: Vec<String>,
    friend_avatars: HashMap<String, String>,
    friend_online: HashMap<String, bool>,
    requests: Vec<IncomingFriendRequest>,
}

#[derive(Serialize)]
struct ProfileSocialResponse {
    followers: Vec<String>,
    following: Vec<String>,
    is_following: bool,
    posts: usize,
}

#[derive(Deserialize)]
struct SetFollowPayload {
    following: bool,
}

#[derive(Serialize)]
struct IncomingFriendRequest {
    id: String,
    nick: String,
}

#[derive(Deserialize)]
struct CreateFriendRequestPayload {
    nick: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct CreateStoryPayload {
    text: String,
    #[serde(default)]
    image: Option<String>,
    #[serde(default)]
    video: Option<String>,
}

#[derive(Serialize)]
struct SessionUser {
    nick: String,
    email: String,
    avatar_url: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct Session {
    email: String,
}

#[derive(Deserialize)]
struct TurnstileVerification {
    success: bool,
}

// --- SUNUCU DURUMU (APP STATE) ---

struct AppState {
    posts: Mutex<Vec<PostModel>>,
    notifications: Mutex<Vec<NotificationModel>>,
    database: Mutex<Database>,
    group_channels: Mutex<HashMap<String, broadcast::Sender<String>>>,
    group_presence: Mutex<HashMap<u64, HashMap<String, LiveGroupMember>>>,
    group_ws_tickets: Mutex<HashMap<String, GroupWebSocketTicket>>,
    user_presence: Mutex<HashMap<String, i64>>,
    tx: broadcast::Sender<String>,
}

#[tokio::main]
async fn main() {
    let (tx, _rx) = broadcast::channel(100);
    let database_path = database_path();
    let mut database = match fs::read_to_string(&database_path) {
        Ok(contents) => serde_json::from_str(&contents).expect("data.json geçerli JSON olmalı"),
        Err(error) if error.kind() == ErrorKind::NotFound => Database::default(),
        Err(error) => panic!("data.json okunamadı: {error}"),
    };
    let user_emails: HashSet<String> = database
        .users
        .iter()
        .filter(|user| !user.disabled)
        .map(|user| user.email.clone())
        .collect();
    database
        .sessions
        .retain(|_, session| user_emails.contains(&session.email));
    let mut groups_migrated = false;
    if database.groups.is_empty() {
        database.groups = default_groups();
        groups_migrated = true;
    } else {
        for group in &mut database.groups {
            if group.categories.is_empty() {
                group.categories = default_groups()[0].categories.clone();
                groups_migrated = true;
            }
        }
    }
    if groups_migrated {
        persist_database(&database)
            .await
            .expect("Topluluklar kalıcı veriye yazılamadı");
    }

    // Başlangıç Örnek Verileri (Görseldekine Uygun)
    let initial_posts = vec![
        PostModel {
            id: 1,
            author: "Skylex".into(),
            content: "Gecenin sessizliği... 🌃👑".into(),
            likes: 124,
            created_at: "2 saat önce".into(),
        },
        PostModel {
            id: 2,
            author: "Zeynep".into(),
            content: "Yeni bir başlangıç... 🚀".into(),
            likes: 42,
            created_at: "4 saat önce".into(),
        },
    ];

    let initial_notifications = vec![
        NotificationModel {
            id: 1,
            title: "Yeni İstek".into(),
            content: "Ali Yılmaz sana arkadaşlık isteği gönderdi.".into(),
        },
        NotificationModel {
            id: 2,
            title: "Sistem".into(),
            content: "RustCord Red sürümüne hoş geldin!".into(),
        },
    ];

    let state = Arc::new(AppState {
        posts: Mutex::new(initial_posts),
        notifications: Mutex::new(initial_notifications),
        database: Mutex::new(database),
        group_channels: Mutex::new(HashMap::new()),
        group_presence: Mutex::new(HashMap::new()),
        group_ws_tickets: Mutex::new(HashMap::new()),
        user_presence: Mutex::new(HashMap::new()),
        tx,
    });

    let cleanup_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let mut database = cleanup_state.database.lock().await;
            let previous_length = database.stories.len();
            database
                .stories
                .retain(|story| !story_expired(story, now_unix()));
            if database.stories.len() != previous_length {
                if let Err(error) = persist_database(&database).await {
                    eprintln!("Süresi dolan hikayeler kaydedilemedi: {error}");
                }
            }
        }
    });

    // API Rotaları (Sağ Menü Butonlarının Çalışması İçin)
    let app = Router::new()
        // 🏠 EV / AKIŞ ROUTELERİ
        .route("/api/posts", get(get_posts).post(create_post))
        .route("/api/posts/:id/like", post(like_post))
        // 👥 GRUPLAR ROUTELERİ
        .route("/api/groups", get(get_groups).post(create_group))
        .route("/api/groups/:group_id", delete(delete_group))
        .route("/api/groups/:group_id/channels", post(add_group_channel))
        .route(
            "/api/groups/:group_id/channels/:channel_id",
            delete(delete_group_channel),
        )
        .route(
            "/api/groups/:group_id/moderators",
            post(assign_group_moderator),
        )
        // 🔔 BİLDİRİMLER ROUTELERİ
        .route("/api/notifications", get(get_notifications))
        .route("/api/config/turnstile", get(get_turnstile_config))
        .route("/api/turnstile/verify", post(verify_turnstile))
        .route("/api/auth", post(authenticate))
        .route("/api/auth/me", get(get_session_user))
        .route(
            "/api/auth/websocket-ticket",
            post(create_group_websocket_ticket),
        )
        .route("/api/auth/avatar", post(update_avatar))
        .route("/api/auth/logout", post(logout))
        .route("/api/config/giphy", get(get_giphy_config))
        .route("/api/youtube/search", get(search_youtube_videos))
        .route(
            "/api/youtube/recommendations",
            get(get_youtube_recommendations),
        )
        .route("/api/videos/collections", get(get_video_collections))
        .route(
            "/api/videos/collections/:collection",
            post(update_video_collection),
        )
        .route("/api/auth/password", post(change_password))
        .route("/api/auth/nickname", post(change_nickname))
        .route("/api/auth/deactivate", post(deactivate_account))
        .route("/api/auth/account", delete(delete_account))
        .route("/api/friends", get(get_friends))
        .route("/api/friends/requests", post(create_friend_request))
        .route("/api/presence", post(update_user_presence))
        .route("/api/profiles/:nick", get(get_profile_social))
        .route("/api/profiles/:nick/follow", post(set_profile_follow))
        .route(
            "/api/dm",
            get(get_direct_messages).post(send_direct_message),
        )
        .route(
            "/api/friends/requests/:request_id/accept",
            post(accept_friend_request),
        )
        .route("/api/stories", get(get_stories).post(create_story))
        // 💬 WEBSOCKET (ANLIK CHAT & MESAJLAR)
        .route("/ws/chat", get(websocket_handler))
        .route("/ws/groups/:group_id", get(group_websocket_handler))
        .route(
            "/ws/groups/:group_id/:channel_id",
            get(group_channel_websocket_handler),
        )
        .fallback_service(ServeDir::new("Client").append_index_html_on_directories(true))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let port = env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(3000);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("🚀 RustCord Red Backend Aktif: http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// --- HANDLER FONKSİYONLARI ---

async fn get_turnstile_config() -> Json<TurnstileConfig> {
    let site_key = env::var("TURNSTILE_SITE_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let has_secret = env::var("TURNSTILE_SECRET_KEY")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    Json(TurnstileConfig {
        enabled: site_key.is_some() && has_secret,
        site_key,
    })
}

async fn search_youtube_videos(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<YouTubeSearchQuery>,
) -> Result<Json<Vec<YouTubeVideo>>, (StatusCode, String)> {
    get_authenticated_email(&headers, &state).await?;
    let search_term = query.q.trim();
    if search_term.is_empty() || search_term.chars().count() > 120 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Arama sözü 1-120 karakter olmalı.".into(),
        ));
    }
    Ok(Json(
        fetch_youtube_videos(search_term, "short", "relevance").await?,
    ))
}

async fn get_giphy_config() -> Json<GiphyConfig> {
    Json(GiphyConfig {
        api_key: env::var("GIPHY_API_KEY")
            .ok()
            .filter(|key| !key.trim().is_empty()),
    })
}

async fn get_youtube_recommendations(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<YouTubeRecommendationQuery>,
) -> Result<Json<Vec<YouTubeVideo>>, (StatusCode, String)> {
    get_authenticated_email(&headers, &state).await?;
    let search_term = match query.topic.as_str() {
        "gaming" => "trending gaming videos",
        "music" => "trending music videos",
        "comedy" => "popular comedy videos",
        "technology" => "latest technology videos",
        "sports" => "popular sports videos",
        "turkey" => "Türkiye trend videolar",
        _ => {
            return Err((StatusCode::BAD_REQUEST, "Geçersiz öneri kategorisi.".into()));
        }
    };
    Ok(Json(
        fetch_youtube_videos(search_term, "any", "date").await?,
    ))
}

async fn fetch_youtube_videos(
    search_term: &str,
    duration: &str,
    order: &str,
) -> Result<Vec<YouTubeVideo>, (StatusCode, String)> {
    let api_key = env::var("YOUTUBE_API_KEY")
        .ok()
        .filter(|key| !key.trim().is_empty())
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "YouTube araması için YOUTUBE_API_KEY ayarlanmalı.".into(),
            )
        })?;
    let upstream = reqwest::Client::new()
        .get("https://www.googleapis.com/youtube/v3/search")
        .query(&[
            ("part", "snippet"),
            ("type", "video"),
            ("videoDuration", duration),
            ("order", order),
            ("regionCode", "TR"),
            ("relevanceLanguage", "tr"),
            ("maxResults", "12"),
            ("q", search_term),
            ("key", api_key.as_str()),
        ])
        .send()
        .await
        .map_err(|error| {
            eprintln!(
                "YouTube arama servisine ulaşılamadı (timeout={}, connect={}).",
                error.is_timeout(),
                error.is_connect()
            );
            (
                StatusCode::BAD_GATEWAY,
                "YouTube arama servisine şu anda ulaşılamıyor.".into(),
            )
        })?;
    if !upstream.status().is_success() {
        let status = upstream.status();
        let error_body = upstream
            .json::<serde_json::Value>()
            .await
            .unwrap_or(serde_json::Value::Null);
        let reason = error_body
            .pointer("/error/errors/0/reason")
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                error_body
                    .pointer("/error/status")
                    .and_then(serde_json::Value::as_str)
            })
            .unwrap_or("unknown");
        let google_message = error_body
            .pointer("/error/message")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("no details");
        eprintln!(
            "YouTube araması HTTP {} yanıtı verdi (reason={reason}, message={google_message}).",
            status
        );
        let message = match (status.as_u16(), reason) {
            (_, "quotaExceeded" | "dailyLimitExceeded") => {
                "YouTube API anahtarının kotası dolmuş. Yenisini ayarlayıp sunucuyu yeniden başlat."
            }
            (400, "keyInvalid" | "API_KEY_INVALID") | (403, "keyInvalid") => {
                "YouTube API anahtarı geçersiz."
            }
            (403, "accessNotConfigured") => {
                "YouTube Data API v3 bu anahtarın projesinde etkin değil."
            }
            (400, _) => {
                "YouTube isteği geçersiz. Arama terimini ve API parametrelerini kontrol et."
            }
            (403, _) => {
                "YouTube API isteği reddedildi. Anahtar kısıtlamalarını ve API izinlerini kontrol et."
            }
            (429, _) => "YouTube API istek sınırına ulaşıldı. Biraz sonra tekrar dene.",
            _ => "YouTube araması başarısız oldu. API anahtarını ve kota durumunu kontrol et.",
        };
        return Err((StatusCode::BAD_GATEWAY, message.into()));
    }
    let response = upstream
        .json::<YouTubeSearchResponse>()
        .await
        .map_err(|error| {
            eprintln!("YouTube arama yanıtı çözümlenemedi: {}", error.is_decode());
            (
                StatusCode::BAD_GATEWAY,
                "YouTube arama sonuçları okunamadı.".into(),
            )
        })?;

    let videos = response
        .items
        .into_iter()
        .filter_map(|item| {
            let id = item.id.video_id?;
            if !id.chars().all(|character| {
                character.is_ascii_alphanumeric() || character == '_' || character == '-'
            }) {
                return None;
            }
            let thumbnail_url = item
                .snippet
                .thumbnails
                .high
                .or(item.snippet.thumbnails.medium)
                .or(item.snippet.thumbnails.default)
                .map(|thumbnail| thumbnail.url)
                .unwrap_or_default();
            Some(YouTubeVideo {
                id,
                title: item.snippet.title,
                description: item.snippet.description,
                channel_title: item.snippet.channel_title,
                thumbnail_url,
            })
        })
        .collect();
    Ok(videos)
}

async fn verify_turnstile(
    Json(payload): Json<TurnstilePayload>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    verify_turnstile_token(&payload.token).await?;
    Ok(Json(serde_json::json!({ "success": true })))
}

async fn verify_turnstile_token(token: &str) -> Result<(), (StatusCode, String)> {
    if token.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Turnstile doğrulaması gerekli.".into(),
        ));
    }
    let secret = env::var("TURNSTILE_SECRET_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Turnstile sunucu anahtarı yapılandırılmamış.".into(),
            )
        })?;

    let response = reqwest::Client::new()
        .post("https://challenges.cloudflare.com/turnstile/v0/siteverify")
        .form(&[("secret", secret.as_str()), ("response", token)])
        .send()
        .await
        .map_err(|_| {
            (
                StatusCode::BAD_GATEWAY,
                "Turnstile doğrulama servisine ulaşılamadı.".into(),
            )
        })?
        .error_for_status()
        .map_err(|_| {
            (
                StatusCode::BAD_GATEWAY,
                "Turnstile doğrulama servisi hata döndürdü.".into(),
            )
        })?
        .json::<TurnstileVerification>()
        .await
        .map_err(|_| {
            (
                StatusCode::BAD_GATEWAY,
                "Turnstile yanıtı okunamadı.".into(),
            )
        })?;

    if !response.success {
        return Err((
            StatusCode::UNAUTHORIZED,
            "Robot doğrulaması başarısız. Lütfen tekrar deneyin.".into(),
        ));
    }

    Ok(())
}

fn validate_email(email: &str) -> bool {
    if email.len() > 254 || email.chars().any(char::is_whitespace) {
        return false;
    }
    let Some((local, domain)) = email.split_once('@') else {
        return false;
    };
    !local.is_empty()
        && !domain.is_empty()
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !email[local.len() + 1..].contains('@')
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn story_expired(story: &StoryModel, now: i64) -> bool {
    story.expires_at <= now
}

fn can_view_story(author_email: &str, viewer_email: &str, friends: &HashSet<String>) -> bool {
    author_email == viewer_email || friends.contains(author_email)
}

fn is_user_online(last_seen: Option<&i64>, now: i64) -> bool {
    last_seen.is_some_and(|last_seen| now >= *last_seen && now.saturating_sub(*last_seen) <= 90)
}

fn are_friends(database: &Database, first_email: &str, second_email: &str) -> bool {
    database.friendships.iter().any(|friendship| {
        (friendship.user_a == first_email && friendship.user_b == second_email)
            || (friendship.user_b == first_email && friendship.user_a == second_email)
    })
}

async fn persist_database(database: &Database) -> Result<(), String> {
    let path = database_path();
    if let Some(parent_dir) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(parent_dir)
            .await
            .map_err(|error| format!("Veri dizini oluşturulamadı: {error}"))?;
    }

    let contents = serde_json::to_vec_pretty(database).map_err(|error| error.to_string())?;
    tokio::fs::write(&path, contents)
        .await
        .map_err(|error| format!("Veri dosyası yazılamadı ({path:?}): {error}"))?;

    Ok(())
}

fn database_path() -> PathBuf {
    let configured_path = env::var_os("APP_DATA_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("app-data.json"));
    if configured_path.is_absolute() {
        configured_path
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(configured_path)
    }
}

fn hash_password(password: &str) -> Result<String, (StatusCode, String)> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Şifre güvenli biçimde kaydedilemedi.".into(),
            )
        })
}

fn verify_password(password: &str, password_hash: &str) -> bool {
    let Ok(parsed_hash) = PasswordHash::new(password_hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok()
}

async fn authenticate(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<AuthPayload>,
) -> Result<(HeaderMap, Json<AuthResponse>), (StatusCode, String)> {
    let nick = payload.nick.trim();
    let email = payload.email.trim().to_lowercase();
    if nick.is_empty() || nick.chars().count() > 32 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Kullanıcı adı 1-32 karakter olmalı.".into(),
        ));
    }
    if !validate_email(&email) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Geçerli biçimde bir e-posta adresi girin.".into(),
        ));
    }
    if !(8..=128).contains(&payload.password.chars().count()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Şifre 8-128 karakter olmalı.".into(),
        ));
    }
    verify_turnstile_token(&payload.turnstile_token).await?;

    let mut database = state.database.lock().await;
    let user = if let Some(user) = database.users.iter().find(|user| user.email == email) {
        if user.disabled || !verify_password(&payload.password, &user.password_hash) {
            return Err((
                StatusCode::UNAUTHORIZED,
                "E-posta veya şifre hatalı.".into(),
            ));
        }
        user.clone()
    } else {
        if database
            .users
            .iter()
            .any(|user| user.nick.eq_ignore_ascii_case(nick))
        {
            return Err((
                StatusCode::CONFLICT,
                "Bu kullanıcı adı zaten kullanılıyor.".into(),
            ));
        }
        let new_user = User {
            email: email.clone(),
            nick: nick.to_string(),
            password_hash: hash_password(&payload.password)?,
            disabled: false,
            avatar_url: None,
        };
        database.users.push(new_user.clone());
        persist_database(&database).await.map_err(|error| {
            eprintln!("Yeni kullanıcı kaydedilemedi: {error}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Hesap kaydedilemedi.".into(),
            )
        })?;
        new_user
    };
    let session = Uuid::new_v4().to_string();
    database.sessions.insert(
        session.clone(),
        Session {
            email: user.email.clone(),
        },
    );
    persist_database(&database).await.map_err(|error| {
        eprintln!("Oturum kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Oturum kalıcı olarak kaydedilemedi.".into(),
        )
    })?;
    let secure_cookie = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|origin| origin.starts_with("https://"))
        || headers
            .get("x-forwarded-proto")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|protocol| protocol.eq_ignore_ascii_case("https"));
    let cookie = format!(
        "mortex_session={session}; Max-Age=31536000; Path=/; HttpOnly; SameSite=Lax{}",
        if secure_cookie { "; Secure" } else { "" }
    );
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&cookie).map_err(|error| {
            eprintln!("Oturum çerezi hazırlanamadı: {error}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Oturum tarayıcıya güvenli biçimde aktarılamadı.".into(),
            )
        })?,
    );
    Ok((
        response_headers,
        Json(AuthResponse {
            session,
            nick: user.nick,
            email: user.email,
            avatar_url: user.avatar_url,
        }),
    ))
}

fn session_token_from_headers(headers: &HeaderMap) -> Option<String> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if let Some(token) = bearer {
        return Some(token.to_string());
    }

    headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookies| {
            cookies.split(';').find_map(|cookie| {
                let (name, value) = cookie.trim().split_once('=')?;
                (name == "mortex_session" && !value.is_empty()).then(|| value.to_string())
            })
        })
}

async fn get_authenticated_email(
    headers: &HeaderMap,
    state: &AppState,
) -> Result<String, (StatusCode, String)> {
    let token = session_token_from_headers(headers)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "Oturum açmanız gerekiyor.".into()))?;
    let database = state.database.lock().await;
    let session = database.sessions.get(&token).cloned().ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            "Oturum geçersiz. Lütfen yeniden giriş yapın.".into(),
        )
    })?;
    Ok(session.email)
}

#[derive(Serialize)]
struct GroupWebSocketTicketResponse {
    ticket: String,
}

async fn create_group_websocket_ticket(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<GroupWebSocketTicketResponse>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let ticket = Uuid::new_v4().to_string();
    let now = now_unix();
    let mut tickets = state.group_ws_tickets.lock().await;
    tickets.retain(|_, ticket| ticket.expires_at > now);
    tickets.insert(
        ticket.clone(),
        GroupWebSocketTicket {
            email,
            expires_at: now + 60,
        },
    );
    Ok(Json(GroupWebSocketTicketResponse { ticket }))
}

async fn get_session_user(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<SessionUser>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let database = state.database.lock().await;
    let user = database
        .users
        .iter()
        .find(|user| user.email == email && !user.disabled)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "Hesap bulunamadı.".into()))?;
    Ok(Json(SessionUser {
        nick: user.nick.clone(),
        email: user.email.clone(),
        avatar_url: user.avatar_url.clone(),
    }))
}

fn valid_avatar_data_url(value: &str) -> bool {
    const MAX_ENCODED_LENGTH: usize = 699_052;
    let Some((mime_type, encoded)) = value.split_once(";base64,").and_then(|(header, data)| {
        header
            .strip_prefix("data:")
            .map(|mime_type| (mime_type, data))
    }) else {
        return false;
    };
    if !matches!(mime_type, "image/png" | "image/jpeg" | "image/webp")
        || encoded.is_empty()
        || encoded.len() > MAX_ENCODED_LENGTH
        || encoded.len() % 4 != 0
    {
        return false;
    }
    let padding_index = encoded.find('=').unwrap_or(encoded.len());
    if encoded[padding_index..].len() > 2 {
        return false;
    }
    encoded[..padding_index]
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/'))
        && encoded[padding_index..].bytes().all(|byte| byte == b'=')
}

async fn update_avatar(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<AvatarPayload>,
) -> Result<Json<SessionUser>, (StatusCode, String)> {
    if payload
        .avatar_url
        .as_deref()
        .is_some_and(|avatar_url| !valid_avatar_data_url(avatar_url))
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "Profil fotoğrafı PNG, JPG veya WebP olmalı ve 512 KB'ı geçmemeli.".into(),
        ));
    }
    let email = get_authenticated_email(&headers, &state).await?;
    let mut database = state.database.lock().await;
    let user = database
        .users
        .iter_mut()
        .find(|user| user.email == email && !user.disabled)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "Hesap bulunamadı.".into()))?;
    user.avatar_url = payload.avatar_url;
    let response = SessionUser {
        nick: user.nick.clone(),
        email: user.email.clone(),
        avatar_url: user.avatar_url.clone(),
    };
    persist_database(&database).await.map_err(|error| {
        eprintln!("Profil fotoğrafı kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Profil fotoğrafı kaydedilemedi.".into(),
        )
    })?;
    let affected_group_ids: Vec<_> = database
        .groups
        .iter()
        .filter(|group| {
            group.member_emails.contains(&email)
                || group.owner_email.as_deref() == Some(email.as_str())
                || group
                    .moderator_emails
                    .iter()
                    .any(|moderator| moderator == &email)
        })
        .map(|group| group.id)
        .collect();
    drop(database);
    for group_id in affected_group_ids {
        broadcast_group_presence(&state, group_id).await;
    }
    Ok(Json(response))
}

async fn logout(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<(HeaderMap, StatusCode), (StatusCode, String)> {
    if let Some(token) = session_token_from_headers(&headers) {
        let mut database = state.database.lock().await;
        database.sessions.remove(&token);
        persist_database(&database).await.map_err(|error| {
            eprintln!("Oturum kapatılamadı: {error}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Oturum kapatılırken kayıt güncellenemedi.".into(),
            )
        })?;
    }
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::SET_COOKIE,
        HeaderValue::from_static("mortex_session=; Max-Age=0; Path=/; HttpOnly; SameSite=Lax"),
    );
    Ok((response_headers, StatusCode::NO_CONTENT))
}

async fn change_password(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ChangePasswordPayload>,
) -> Result<StatusCode, (StatusCode, String)> {
    if !(8..=128).contains(&payload.new_password.chars().count()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Yeni şifre 8-128 karakter olmalı.".into(),
        ));
    }
    let email = get_authenticated_email(&headers, &state).await?;
    let mut database = state.database.lock().await;
    let user = database
        .users
        .iter_mut()
        .find(|user| user.email == email && !user.disabled)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "Hesap bulunamadı.".into()))?;
    if !verify_password(&payload.current_password, &user.password_hash) {
        return Err((StatusCode::UNAUTHORIZED, "Mevcut şifre hatalı.".into()));
    }
    user.password_hash = hash_password(&payload.new_password)?;
    persist_database(&database).await.map_err(|error| {
        eprintln!("Şifre değişikliği kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Şifre değişikliği kaydedilemedi.".into(),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn change_nickname(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<NicknamePayload>,
) -> Result<StatusCode, (StatusCode, String)> {
    let nick = payload.nick.trim();
    if nick.is_empty() || nick.chars().count() > 32 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Kullanıcı adı 1-32 karakter olmalı.".into(),
        ));
    }
    let email = get_authenticated_email(&headers, &state).await?;
    let mut database = state.database.lock().await;
    if database
        .users
        .iter()
        .any(|user| user.email != email && user.nick.eq_ignore_ascii_case(nick))
    {
        return Err((
            StatusCode::CONFLICT,
            "Bu kullanıcı adı zaten kullanılıyor.".into(),
        ));
    }
    let user = database
        .users
        .iter_mut()
        .find(|user| user.email == email && !user.disabled)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "Hesap bulunamadı.".into()))?;
    user.nick = nick.to_string();
    persist_database(&database).await.map_err(|error| {
        eprintln!("Kullanıcı adı kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Kullanıcı adı kaydedilemedi.".into(),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn deactivate_account(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let mut database = state.database.lock().await;
    let user = database
        .users
        .iter_mut()
        .find(|user| user.email == email)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Hesap bulunamadı.".into()))?;
    user.disabled = true;
    database
        .sessions
        .retain(|_, session| session.email != email);
    persist_database(&database).await.map_err(|error| {
        eprintln!("Hesap durumu kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Hesap durumu kaydedilemedi.".into(),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_account(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let mut database = state.database.lock().await;
    database.users.retain(|user| user.email != email);
    database
        .friend_requests
        .retain(|request| request.from_email != email && request.to_email != email);
    database
        .friendships
        .retain(|friendship| friendship.user_a != email && friendship.user_b != email);
    database
        .follows
        .retain(|follow| follow.follower_email != email && follow.followed_email != email);
    database.stories.retain(|story| story.author_email != email);
    database
        .direct_messages
        .retain(|message| message.from_email != email && message.to_email != email);
    database
        .video_collections
        .retain(|collection| collection.email != email);
    database
        .sessions
        .retain(|_, session| session.email != email);
    persist_database(&database).await.map_err(|error| {
        eprintln!("Hesap silinemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Hesap silinemedi.".into(),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_friends(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<FriendListResponse>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let now = now_unix();
    let online_users = state.user_presence.lock().await;
    let database = state.database.lock().await;
    let friend_profiles: Vec<(String, Option<String>, bool)> = database
        .friendships
        .iter()
        .filter_map(|friendship| {
            let friend_email = if friendship.user_a == email {
                &friendship.user_b
            } else if friendship.user_b == email {
                &friendship.user_a
            } else {
                return None;
            };
            database
                .users
                .iter()
                .find(|user| user.email == *friend_email && !user.disabled)
                .map(|user| {
                    (
                        user.nick.clone(),
                        user.avatar_url.clone(),
                        is_user_online(online_users.get(&user.email), now),
                    )
                })
        })
        .collect();
    let friends = friend_profiles
        .iter()
        .map(|(nick, _, _)| nick.clone())
        .collect();
    let friend_avatars = friend_profiles
        .iter()
        .filter_map(|(nick, avatar_url, _)| {
            avatar_url
                .clone()
                .map(|avatar_url| (nick.clone(), avatar_url))
        })
        .collect();
    let friend_online = friend_profiles
        .into_iter()
        .map(|(nick, _, online)| (nick, online))
        .collect();
    let requests = database
        .friend_requests
        .iter()
        .filter(|request| request.to_email == email)
        .filter_map(|request| {
            database
                .users
                .iter()
                .find(|user| user.email == request.from_email && !user.disabled)
                .map(|user| IncomingFriendRequest {
                    id: request.id.clone(),
                    nick: user.nick.clone(),
                })
        })
        .collect();
    Ok(Json(FriendListResponse {
        friends,
        friend_avatars,
        friend_online,
        requests,
    }))
}

async fn update_user_presence(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let now = now_unix();
    let mut presence = state.user_presence.lock().await;
    presence.retain(|_, last_seen| now.saturating_sub(*last_seen) <= 300);
    presence.insert(email, now);
    Ok(StatusCode::NO_CONTENT)
}

fn profile_social_response(
    database: &Database,
    viewer_email: &str,
    profile_email: &str,
    posts: usize,
) -> ProfileSocialResponse {
    let active_users: HashMap<_, _> = database
        .users
        .iter()
        .filter(|user| !user.disabled)
        .map(|user| (user.email.as_str(), user.nick.as_str()))
        .collect();
    let followers = database
        .follows
        .iter()
        .filter(|follow| follow.followed_email == profile_email)
        .filter_map(|follow| active_users.get(follow.follower_email.as_str()))
        .map(|nick| (*nick).to_string())
        .collect();
    let following = database
        .follows
        .iter()
        .filter(|follow| follow.follower_email == profile_email)
        .filter_map(|follow| active_users.get(follow.followed_email.as_str()))
        .map(|nick| (*nick).to_string())
        .collect();
    ProfileSocialResponse {
        followers,
        following,
        is_following: database.follows.iter().any(|follow| {
            follow.follower_email == viewer_email && follow.followed_email == profile_email
        }),
        posts,
    }
}

async fn get_profile_social(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(nick): Path<String>,
) -> Result<Json<ProfileSocialResponse>, (StatusCode, String)> {
    let viewer_email = get_authenticated_email(&headers, &state).await?;
    let database = state.database.lock().await;
    let profile = database
        .users
        .iter()
        .find(|user| user.nick.eq_ignore_ascii_case(&nick) && !user.disabled)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Profil bulunamadı.".into()))?;
    let posts = state
        .posts
        .lock()
        .await
        .iter()
        .filter(|post| post.author.eq_ignore_ascii_case(&profile.nick))
        .count();
    Ok(Json(profile_social_response(
        &database,
        &viewer_email,
        &profile.email,
        posts,
    )))
}

async fn set_profile_follow(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(nick): Path<String>,
    Json(payload): Json<SetFollowPayload>,
) -> Result<Json<ProfileSocialResponse>, (StatusCode, String)> {
    let follower_email = get_authenticated_email(&headers, &state).await?;
    let mut database = state.database.lock().await;
    let followed_email = database
        .users
        .iter()
        .find(|user| user.nick.eq_ignore_ascii_case(&nick) && !user.disabled)
        .map(|user| user.email.clone())
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Profil bulunamadı.".into()))?;
    if follower_email == followed_email {
        return Err((StatusCode::BAD_REQUEST, "Kendini takip edemezsin.".into()));
    }
    if payload.following {
        if !database.follows.iter().any(|follow| {
            follow.follower_email == follower_email && follow.followed_email == followed_email
        }) {
            database.follows.push(Follow {
                follower_email: follower_email.clone(),
                followed_email: followed_email.clone(),
            });
        }
    } else {
        database.follows.retain(|follow| {
            follow.follower_email != follower_email || follow.followed_email != followed_email
        });
    }
    persist_database(&database).await.map_err(|error| {
        eprintln!("Takip değişikliği kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Takip değişikliği kaydedilemedi.".into(),
        )
    })?;
    let posts = state
        .posts
        .lock()
        .await
        .iter()
        .filter(|post| {
            database.users.iter().any(|user| {
                user.email == followed_email && post.author.eq_ignore_ascii_case(&user.nick)
            })
        })
        .count();
    Ok(Json(profile_social_response(
        &database,
        &follower_email,
        &followed_email,
        posts,
    )))
}

async fn get_video_collections(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<VideoCollectionsResponse>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let database = state.database.lock().await;
    let collections = database
        .video_collections
        .iter()
        .find(|collections| collections.email == email);
    Ok(Json(VideoCollectionsResponse {
        liked: collections.map_or_else(Vec::new, |collections| collections.liked.clone()),
        favorites: collections.map_or_else(Vec::new, |collections| collections.favorites.clone()),
    }))
}

async fn update_video_collection(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(collection_name): Path<String>,
    Json(payload): Json<UpdateVideoCollectionPayload>,
) -> Result<Json<VideoCollectionsResponse>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    if !matches!(collection_name.as_str(), "liked" | "favorites") {
        return Err((
            StatusCode::BAD_REQUEST,
            "Video listesi liked veya favorites olmalı.".into(),
        ));
    }
    if !matches!(payload.video.provider.as_str(), "youtube" | "instagram")
        || payload.video.id.is_empty()
        || payload.video.id.len() > 128
        || !payload
            .video
            .id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        || payload.video.title.len() > 300
        || payload.video.channel_title.len() > 160
        || payload.video.thumbnail_url.len() > 2048
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "Video bilgileri geçersiz veya çok uzun.".into(),
        ));
    }

    let mut database = state.database.lock().await;
    let collections = database
        .video_collections
        .iter_mut()
        .find(|collections| collections.email == email);
    let collections = match collections {
        Some(collections) => collections,
        None => {
            database.video_collections.push(UserVideoCollections {
                email: email.clone(),
                ..UserVideoCollections::default()
            });
            database
                .video_collections
                .last_mut()
                .expect("collection inserted")
        }
    };
    let videos = if collection_name == "liked" {
        &mut collections.liked
    } else {
        &mut collections.favorites
    };
    let existing = videos
        .iter()
        .position(|video| video.provider == payload.video.provider && video.id == payload.video.id);
    if payload.saved {
        if let Some(index) = existing {
            videos[index] = payload.video;
        } else {
            videos.push(payload.video);
        }
    } else if let Some(index) = existing {
        videos.remove(index);
    }

    persist_database(&database).await.map_err(|error| {
        eprintln!("Video listesi kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Video listesi kaydedilemedi.".into(),
        )
    })?;
    let collections = database
        .video_collections
        .iter()
        .find(|collections| collections.email == email)
        .expect("collection exists after update");
    Ok(Json(VideoCollectionsResponse {
        liked: collections.liked.clone(),
        favorites: collections.favorites.clone(),
    }))
}

async fn create_friend_request(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateFriendRequestPayload>,
) -> Result<StatusCode, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let target_nick = payload.nick.trim();
    let mut database = state.database.lock().await;
    let target_email = database
        .users
        .iter()
        .find(|user| user.nick.eq_ignore_ascii_case(target_nick) && !user.disabled)
        .map(|user| user.email.clone())
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Bu kullanıcı bulunamadı.".into()))?;
    if target_email == email {
        return Err((
            StatusCode::BAD_REQUEST,
            "Kendine arkadaşlık isteği gönderemezsin.".into(),
        ));
    }
    if database.friendships.iter().any(|friendship| {
        (friendship.user_a == email && friendship.user_b == target_email)
            || (friendship.user_b == email && friendship.user_a == target_email)
    }) {
        return Err((StatusCode::CONFLICT, "Bu kullanıcı zaten arkadaşın.".into()));
    }
    if database.friend_requests.iter().any(|request| {
        (request.from_email == email && request.to_email == target_email)
            || (request.from_email == target_email && request.to_email == email)
    }) {
        return Err((
            StatusCode::CONFLICT,
            "Bu kullanıcıyla bekleyen bir arkadaşlık isteği var.".into(),
        ));
    }
    database.friend_requests.push(FriendRequest {
        id: Uuid::new_v4().to_string(),
        from_email: email,
        to_email: target_email,
    });
    persist_database(&database).await.map_err(|error| {
        eprintln!("Arkadaşlık isteği kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Arkadaşlık isteği gönderilemedi.".into(),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn accept_friend_request(
    Path(request_id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let mut database = state.database.lock().await;
    let index = database
        .friend_requests
        .iter()
        .position(|request| request.id == request_id && request.to_email == email)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "Arkadaşlık isteği bulunamadı.".into(),
            )
        })?;
    let request = database.friend_requests.remove(index);
    database.friendships.push(Friendship {
        user_a: email,
        user_b: request.from_email,
    });
    persist_database(&database).await.map_err(|error| {
        eprintln!("Arkadaşlık kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Arkadaşlık onaylanamadı.".into(),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_direct_messages(
    Query(query): Query<DirectMessageQuery>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<DirectMessageResponse>>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let database = state.database.lock().await;
    let recipient = database
        .users
        .iter()
        .find(|user| !user.disabled && user.nick.eq_ignore_ascii_case(query.nick.trim()))
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Kullanıcı bulunamadı.".into()))?;
    let recipient_email = recipient.email.clone();
    if email == recipient_email {
        return Err((
            StatusCode::BAD_REQUEST,
            "Kendinle özel mesajlaşma başlatamazsın.".into(),
        ));
    }
    if !are_friends(&database, &email, &recipient_email) {
        return Err((
            StatusCode::FORBIDDEN,
            "Özel mesajlar yalnızca arkadaşlar arasında gönderilebilir.".into(),
        ));
    }

    let mut messages: Vec<DirectMessageResponse> = database
        .direct_messages
        .iter()
        .filter(|message| {
            (message.from_email == email && message.to_email == recipient_email)
                || (message.from_email == recipient_email && message.to_email == email)
        })
        .map(|message| DirectMessageResponse {
            id: message.id.clone(),
            from_nick: message.from_nick.clone(),
            text: message.text.clone(),
            created_at: message.created_at,
        })
        .collect();
    messages.sort_by_key(|message| message.created_at);
    Ok(Json(messages))
}

async fn send_direct_message(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SendDirectMessagePayload>,
) -> Result<Json<DirectMessageResponse>, (StatusCode, String)> {
    let text = payload.text.trim();
    if text.is_empty() || text.chars().count() > 2000 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Mesaj 1-2000 karakter arasında olmalı.".into(),
        ));
    }
    let email = get_authenticated_email(&headers, &state).await?;
    let mut database = state.database.lock().await;
    let sender = database
        .users
        .iter()
        .find(|user| user.email == email && !user.disabled)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "Hesap bulunamadı.".into()))?;
    let from_nick = sender.nick.clone();
    let recipient = database
        .users
        .iter()
        .find(|user| !user.disabled && user.nick.eq_ignore_ascii_case(payload.to_nick.trim()))
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Kullanıcı bulunamadı.".into()))?;
    let recipient_email = recipient.email.clone();
    if email == recipient_email {
        return Err((
            StatusCode::BAD_REQUEST,
            "Kendine özel mesaj gönderemezsin.".into(),
        ));
    }
    if !are_friends(&database, &email, &recipient_email) {
        return Err((
            StatusCode::FORBIDDEN,
            "Özel mesajlar yalnızca arkadaşlar arasında gönderilebilir.".into(),
        ));
    }

    let message = DirectMessage {
        id: Uuid::new_v4().to_string(),
        from_email: email,
        to_email: recipient_email,
        from_nick,
        text: text.to_string(),
        created_at: now_unix(),
    };
    database.direct_messages.push(message.clone());
    persist_database(&database).await.map_err(|error| {
        eprintln!("Özel mesaj kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Mesaj gönderilemedi.".into(),
        )
    })?;
    Ok(Json(DirectMessageResponse {
        id: message.id,
        from_nick: message.from_nick,
        text: message.text,
        created_at: message.created_at,
    }))
}

async fn get_stories(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<StoryModel>>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let mut database = state.database.lock().await;
    let now = now_unix();
    database.stories.retain(|story| !story_expired(story, now));
    let friends: HashSet<String> = database
        .friendships
        .iter()
        .filter_map(|friendship| {
            if friendship.user_a == email {
                Some(friendship.user_b.clone())
            } else if friendship.user_b == email {
                Some(friendship.user_a.clone())
            } else {
                None
            }
        })
        .collect();
    let stories = database
        .stories
        .iter()
        .filter(|story| can_view_story(&story.author_email, &email, &friends))
        .cloned()
        .collect();
    Ok(Json(stories))
}

async fn create_story(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateStoryPayload>,
) -> Result<Json<StoryModel>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let text = payload.text.trim();
    if text.chars().count() > 500 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Hikaye metni en fazla 500 karakter olabilir.".into(),
        ));
    }
    if text.is_empty() && payload.image.is_none() && payload.video.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Hikaye metni veya medya gerekli.".into(),
        ));
    }

    if let Some(image) = &payload.image {
        let allowed_type = [
            "data:image/png;base64,",
            "data:image/jpeg;base64,",
            "data:image/webp;base64,",
            "data:image/gif;base64,",
        ]
        .iter()
        .any(|prefix| image.starts_with(prefix));
        if !allowed_type {
            return Err((
                StatusCode::BAD_REQUEST,
                "Desteklenmeyen görsel türü.".into(),
            ));
        }
    }

    if let Some(video) = &payload.video {
        let allowed_type = [
            "data:video/mp4;base64,",
            "data:video/webm;base64,",
            "data:video/quicktime;base64,",
        ]
        .iter()
        .any(|prefix| video.starts_with(prefix));
        if !allowed_type {
            return Err((
                StatusCode::BAD_REQUEST,
                "Desteklenmeyen video türü.".into(),
            ));
        }
    }
    let now = now_unix();
    let mut database = state.database.lock().await;
    let author = database
        .users
        .iter()
        .find(|user| user.email == email && !user.disabled)
        .map(|user| user.nick.clone())
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "Hesap bulunamadı.".into()))?;
    let story = StoryModel {
        id: Uuid::new_v4().to_string(),
        author_email: email,
        author,
        text: text.to_string(),
        image: payload.image,
        video: payload.video,
        created_at: now,
        expires_at: now + 24 * 60 * 60,
    };
    database.stories.push(story.clone());
    persist_database(&database).await.map_err(|error| {
        eprintln!("Hikaye kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Hikaye paylaşılamadı.".into(),
        )
    })?;
    Ok(Json(story))
}

// 1. Postları Getir (Ev Sekmesi)
async fn get_posts(State(state): State<Arc<AppState>>) -> Json<Vec<PostModel>> {
    let posts = state.posts.lock().await;
    Json(posts.clone())
}

// 2. Yeni Post Gönder (Resim/Yazı Paylaşımı)
async fn create_post(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreatePostPayload>,
) -> Json<PostModel> {
    let mut posts = state.posts.lock().await;
    let new_post = PostModel {
        id: (posts.len() + 1) as u64,
        author: payload.author,
        content: payload.content,
        likes: 0,
        created_at: "Şimdi".into(),
    };
    posts.insert(0, new_post.clone()); // En başa ekle
    Json(new_post)
}

// 3. Post Beğen (Like Butonu)
async fn like_post(Path(id): Path<u64>, State(state): State<Arc<AppState>>) -> Json<String> {
    let mut posts = state.posts.lock().await;
    if let Some(post) = posts.iter_mut().find(|p| p.id == id) {
        post.likes += 1;
        return Json("Beğenildi!".into());
    }
    Json("Post bulunamadı.".into())
}

fn default_groups() -> Vec<GroupModel> {
    ["Yazılım Topluluğu", "Gamer Odası"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| GroupModel {
            id: index as u64 + 1,
            name: name.into(),
            members_count: if index == 0 { 15 } else { 8 },
            owner_email: None,
            moderator_emails: Vec::new(),
            member_emails: Vec::new(),
            categories: vec![
                ChannelCategory {
                    id: "general".into(),
                    name: "GENEL".into(),
                    channels: vec![GroupChannel {
                        id: "general-chat".into(),
                        name: "genel-sohbet".into(),
                        kind: "text".into(),
                    }],
                },
                ChannelCategory {
                    id: "voice".into(),
                    name: "SES ODALARI".into(),
                    channels: vec![GroupChannel {
                        id: "voice-room".into(),
                        name: "Ses Odası".into(),
                        kind: "voice".into(),
                    }],
                },
            ],
        })
        .collect()
}

fn group_view(group: &GroupModel, email: &str, database: &Database) -> GroupView {
    let role = if group.owner_email.as_deref() == Some(email) {
        "founder"
    } else if group
        .moderator_emails
        .iter()
        .any(|moderator| moderator == email)
    {
        "moderator"
    } else {
        "member"
    };
    let owner = group
        .owner_email
        .as_deref()
        .and_then(|owner_email| database.users.iter().find(|user| user.email == owner_email))
        .map(|user| user.nick.clone())
        .unwrap_or_else(|| "Topluluk kurucusu".into());
    GroupView {
        id: group.id,
        name: group.name.clone(),
        members_count: group.members_count,
        owner,
        role: role.into(),
        categories: group.categories.clone(),
    }
}

fn can_manage_group_channels(group: &GroupModel, email: &str) -> bool {
    group.owner_email.as_deref() == Some(email)
        || group
            .moderator_emails
            .iter()
            .any(|moderator| moderator == email)
}

fn group_has_channel(group: &GroupModel, channel_id: &str) -> bool {
    group
        .categories
        .iter()
        .flat_map(|category| &category.channels)
        .any(|channel| channel.id == channel_id)
}

fn remove_group_channel(group: &mut GroupModel, channel_id: &str) -> bool {
    let mut removed = false;
    group.categories.retain_mut(|category| {
        let original_len = category.channels.len();
        category.channels.retain(|channel| channel.id != channel_id);
        removed |= category.channels.len() != original_len;
        !category.channels.is_empty()
    });
    removed
}

fn can_assign_group_moderators(group: &GroupModel, email: &str) -> bool {
    group.owner_email.as_deref() == Some(email)
}

fn can_delete_group(group: &GroupModel, email: &str) -> bool {
    group.owner_email.as_deref() == Some(email)
}

async fn get_groups(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<GroupView>>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let database = state.database.lock().await;
    Ok(Json(
        database
            .groups
            .iter()
            .map(|group| group_view(group, &email, &database))
            .collect(),
    ))
}

async fn create_group(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateGroupPayload>,
) -> Result<Json<GroupView>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let name = payload.name.trim();
    if name.is_empty() || name.chars().count() > 64 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Grup adı 1-64 karakter olmalı.".into(),
        ));
    }
    let mut database = state.database.lock().await;
    let new_group = GroupModel {
        id: database
            .groups
            .iter()
            .map(|group| group.id)
            .max()
            .unwrap_or(0)
            + 1,
        name: name.to_string(),
        members_count: 1,
        owner_email: Some(email.clone()),
        moderator_emails: Vec::new(),
        member_emails: vec![email.clone()],
        categories: default_groups()[0].categories.clone(),
    };
    database.groups.push(new_group.clone());
    persist_database(&database).await.map_err(|error| {
        eprintln!("Yeni topluluk kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Topluluk kaydedilemedi.".into(),
        )
    })?;
    Ok(Json(group_view(&new_group, &email, &database)))
}

async fn delete_group(
    Path(group_id): Path<u64>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let mut database = state.database.lock().await;
    let group = database
        .groups
        .iter()
        .find(|group| group.id == group_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Grup bulunamadı.".into()))?;
    if !can_delete_group(group, &email) {
        return Err((
            StatusCode::FORBIDDEN,
            "Grubu yalnızca kurucusu silebilir.".into(),
        ));
    }
    database.groups.retain(|group| group.id != group_id);
    persist_database(&database).await.map_err(|error| {
        eprintln!("Grup silinemedi: {error}");
        (StatusCode::INTERNAL_SERVER_ERROR, "Grup silinemedi.".into())
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn add_group_channel(
    Path(group_id): Path<u64>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<AddGroupChannelPayload>,
) -> Result<Json<GroupView>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let name = payload.name.trim();
    let category_name = payload.category.trim();
    if name.is_empty()
        || name.chars().count() > 48
        || category_name.is_empty()
        || category_name.chars().count() > 40
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "Kategori ve kanal adı geçerli olmalı.".into(),
        ));
    }
    if payload.kind != "text" && payload.kind != "voice" {
        return Err((
            StatusCode::BAD_REQUEST,
            "Kanal türü text veya voice olmalı.".into(),
        ));
    }
    let mut database = state.database.lock().await;
    let group = database
        .groups
        .iter_mut()
        .find(|group| group.id == group_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Topluluk bulunamadı.".into()))?;
    if !can_manage_group_channels(group, &email) {
        return Err((
            StatusCode::FORBIDDEN,
            "Kanal eklemek için kurucu veya moderatör olmalısınız.".into(),
        ));
    }
    let category_id = category_name.to_lowercase().replace(' ', "-");
    let category_index = match group
        .categories
        .iter()
        .position(|category| category.name.eq_ignore_ascii_case(category_name))
    {
        Some(index) => index,
        None => {
            group.categories.push(ChannelCategory {
                id: category_id.clone(),
                name: category_name.to_string(),
                channels: Vec::new(),
            });
            group.categories.len() - 1
        }
    };
    let category = &mut group.categories[category_index];
    if category
        .channels
        .iter()
        .any(|channel| channel.name.eq_ignore_ascii_case(name))
    {
        return Err((
            StatusCode::CONFLICT,
            "Bu kategoride aynı isimli kanal zaten var.".into(),
        ));
    }
    category.channels.push(GroupChannel {
        id: format!(
            "group-{group_id}-category-{category_index}-channel-{}",
            category.channels.len() + 1
        ),
        name: name.to_string(),
        kind: payload.kind,
    });
    let group = group.clone();
    persist_database(&database).await.map_err(|error| {
        eprintln!("Topluluk kanalı kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Kanal kaydedilemedi.".into(),
        )
    })?;
    Ok(Json(group_view(&group, &email, &database)))
}

async fn delete_group_channel(
    Path((group_id, channel_id)): Path<(u64, String)>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<GroupView>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let mut database = state.database.lock().await;
    let group = database
        .groups
        .iter_mut()
        .find(|group| group.id == group_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Grup bulunamadı.".into()))?;
    if !can_manage_group_channels(group, &email) {
        return Err((
            StatusCode::FORBIDDEN,
            "Kanal silmek için kurucu veya moderatör olmalısınız.".into(),
        ));
    }
    if !remove_group_channel(group, &channel_id) {
        return Err((StatusCode::NOT_FOUND, "Silinecek kanal bulunamadı.".into()));
    }
    let group = group.clone();
    persist_database(&database).await.map_err(|error| {
        eprintln!("Grup kanalı silinemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Kanal silinemedi.".into(),
        )
    })?;
    Ok(Json(group_view(&group, &email, &database)))
}

async fn assign_group_moderator(
    Path(group_id): Path<u64>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<AssignGroupModeratorPayload>,
) -> Result<Json<GroupView>, (StatusCode, String)> {
    let email = get_authenticated_email(&headers, &state).await?;
    let target_nick = payload.nick.trim();
    if target_nick.is_empty() || target_nick.chars().count() > 32 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Geçerli bir kullanıcı adı girin.".into(),
        ));
    }
    let mut database = state.database.lock().await;
    let target = database
        .users
        .iter()
        .find(|user| user.nick.eq_ignore_ascii_case(target_nick) && !user.disabled)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Kullanıcı bulunamadı.".into()))?;
    let group = database
        .groups
        .iter_mut()
        .find(|group| group.id == group_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Topluluk bulunamadı.".into()))?;
    if !can_assign_group_moderators(group, &email) {
        return Err((
            StatusCode::FORBIDDEN,
            "Moderatörleri yalnızca kurucu atayabilir.".into(),
        ));
    }
    if group.owner_email.as_deref() == Some(target.email.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Kurucu zaten tüm yönetim yetkilerine sahip.".into(),
        ));
    }
    if !group.moderator_emails.contains(&target.email) {
        group.moderator_emails.push(target.email.clone());
    }
    if !group.member_emails.contains(&target.email) {
        group.member_emails.push(target.email);
    }
    let group = group.clone();
    persist_database(&database).await.map_err(|error| {
        eprintln!("Moderatör ataması kaydedilemedi: {error}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Moderatör ataması kaydedilemedi.".into(),
        )
    })?;
    Ok(Json(group_view(&group, &email, &database)))
}

// 6. Bildirimleri Getir (Bilgilendirme / Haber Sekmesi)
async fn get_notifications(State(state): State<Arc<AppState>>) -> Json<Vec<NotificationModel>> {
    let notifications = state.notifications.lock().await;
    Json(notifications.clone())
}

// 7. WebSocket Sohbet Motoru (Mesaj Sekmesi & DM)
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state.tx.clone()))
}

async fn group_websocket_handler(
    Path(group_id): Path<u64>,
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Result<impl axum::response::IntoResponse, StatusCode> {
    if !state
        .database
        .lock()
        .await
        .groups
        .iter()
        .any(|group| group.id == group_id)
    {
        return Err(StatusCode::NOT_FOUND);
    }

    let tx = {
        let mut channels = state.group_channels.lock().await;
        channels
            .entry(format!("{group_id}:legacy"))
            .or_insert_with(|| broadcast::channel(100).0)
            .clone()
    };
    Ok(ws.on_upgrade(move |socket| handle_socket(socket, tx)))
}

async fn group_channel_websocket_handler(
    Path((group_id, channel_id)): Path<(u64, String)>,
    Query(query): Query<GroupWebSocketQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Result<impl axum::response::IntoResponse, StatusCode> {
    let email = match query.ticket {
        Some(ticket) => {
            let mut tickets = state.group_ws_tickets.lock().await;
            take_group_websocket_ticket(&mut tickets, &ticket, now_unix())
                .ok_or(StatusCode::UNAUTHORIZED)?
        }
        None => get_authenticated_email(&headers, &state)
            .await
            .map_err(|_| StatusCode::UNAUTHORIZED)?,
    };
    {
        let mut database = state.database.lock().await;
        let user_exists = database
            .users
            .iter()
            .any(|user| user.email == email && !user.disabled);
        if !user_exists {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let group = database
            .groups
            .iter_mut()
            .find(|group| group.id == group_id)
            .ok_or(StatusCode::NOT_FOUND)?;
        if !group_has_channel(group, &channel_id) {
            return Err(StatusCode::NOT_FOUND);
        }
        let new_member = !group.member_emails.contains(&email);
        if new_member {
            group.member_emails.push(email.clone());
        }
        if new_member {
            persist_database(&database).await.map_err(|error| {
                eprintln!("Grup üyeliği kaydedilemedi: {error}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        }
    }

    let channel_key = format!("{group_id}:{channel_id}");
    let tx = {
        let mut channels = state.group_channels.lock().await;
        channels
            .entry(channel_key)
            .or_insert_with(|| broadcast::channel(100).0)
            .clone()
    };
    Ok(
        ws.on_upgrade(move |socket| {
            handle_group_channel_socket(socket, tx, state, group_id, email)
        }),
    )
}

#[derive(Deserialize)]
struct GroupWebSocketQuery {
    ticket: Option<String>,
}

fn take_group_websocket_ticket(
    tickets: &mut HashMap<String, GroupWebSocketTicket>,
    ticket: &str,
    now: i64,
) -> Option<String> {
    tickets
        .remove(ticket)
        .filter(|ticket| ticket.expires_at > now)
        .map(|ticket| ticket.email)
}

fn group_member_profiles(
    group: &GroupModel,
    users: &[User],
    online_emails: &HashSet<String>,
) -> Vec<GroupPresenceMember> {
    let member_emails: HashSet<_> = group
        .member_emails
        .iter()
        .chain(group.owner_email.iter())
        .chain(group.moderator_emails.iter())
        .collect();
    let mut members: Vec<_> = users
        .iter()
        .filter(|user| !user.disabled && member_emails.contains(&user.email))
        .map(|user| GroupPresenceMember {
            nick: user.nick.clone(),
            avatar_url: user.avatar_url.clone(),
            role: if group.owner_email.as_deref() == Some(user.email.as_str()) {
                "Kurucu"
            } else if group
                .moderator_emails
                .iter()
                .any(|moderator| moderator == &user.email)
            {
                "Moderatör"
            } else {
                "Üye"
            }
            .into(),
            online: online_emails.contains(&user.email),
        })
        .collect();
    members.sort_by(|first, second| {
        second
            .online
            .cmp(&first.online)
            .then_with(|| first.nick.to_lowercase().cmp(&second.nick.to_lowercase()))
    });
    members
}

async fn group_presence_snapshot(state: &AppState, group_id: u64) -> Vec<GroupPresenceMember> {
    let (group, users) = {
        let database = state.database.lock().await;
        let Some(group) = database
            .groups
            .iter()
            .find(|group| group.id == group_id)
            .cloned()
        else {
            return Vec::new();
        };
        let users = database.users.clone();
        (group, users)
    };
    let online_emails: HashSet<_> = state
        .group_presence
        .lock()
        .await
        .get(&group_id)
        .map(|members| members.keys().cloned().collect())
        .unwrap_or_default();
    group_member_profiles(&group, &users, &online_emails)
}

async fn broadcast_group_presence(state: &AppState, group_id: u64) {
    let members = group_presence_snapshot(state, group_id).await;
    let event = serde_json::json!({
        "type": "group-presence",
        "members": members,
    })
    .to_string();
    let senders: Vec<_> = state
        .group_channels
        .lock()
        .await
        .iter()
        .filter(|(channel_key, _)| channel_key.starts_with(&format!("{group_id}:")))
        .map(|(_, sender)| sender.clone())
        .collect();
    for sender in senders {
        let _ = sender.send(event.clone());
    }
}

async fn handle_group_channel_socket(
    socket: WebSocket,
    tx: broadcast::Sender<String>,
    state: Arc<AppState>,
    group_id: u64,
    email: String,
) {
    let connection_id = Uuid::new_v4().to_string();
    let (mut sender, mut receiver) = socket.split();
    let mut rx = tx.subscribe();
    {
        let mut presence = state.group_presence.lock().await;
        let member = presence
            .entry(group_id)
            .or_default()
            .entry(email.clone())
            .or_insert_with(|| LiveGroupMember {
                connections: HashSet::new(),
            });
        member.connections.insert(connection_id.clone());
    }
    broadcast_group_presence(&state, group_id).await;

    let mut send_task = tokio::spawn(async move {
        while let Ok(message) = rx.recv().await {
            if sender.send(Message::Text(message.into())).await.is_err() {
                break;
            }
        }
    });
    let mut receive_task = tokio::spawn(async move {
        while let Some(Ok(Message::Text(text))) = receiver.next().await {
            let _ = tx.send(text.to_string());
        }
    });
    tokio::select! {
        _ = &mut send_task => receive_task.abort(),
        _ = &mut receive_task => send_task.abort(),
    };

    let mut presence = state.group_presence.lock().await;
    if let Some(members) = presence.get_mut(&group_id) {
        if let Some(member) = members.get_mut(&email) {
            member.connections.remove(&connection_id);
            if member.connections.is_empty() {
                members.remove(&email);
            }
        }
        if members.is_empty() {
            presence.remove(&group_id);
        }
    }
    drop(presence);
    broadcast_group_presence(&state, group_id).await;
}

async fn handle_socket(socket: WebSocket, tx: broadcast::Sender<String>) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = tx.subscribe();

    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(Message::Text(text))) = receiver.next().await {
            let _ = tx.send(text);
        }
    });

    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_email_syntax() {
        assert!(validate_email("user@example.com"));
        assert!(!validate_email("user@localhost"));
        assert!(!validate_email("not-an-email"));
        assert!(!validate_email("user @example.com"));
    }

    #[test]
    fn group_websocket_tickets_expire_and_can_only_be_used_once() {
        let mut tickets = HashMap::from([(
            "valid-ticket".to_string(),
            GroupWebSocketTicket {
                email: "user@example.com".into(),
                expires_at: 101,
            },
        )]);
        tickets.insert(
            "expired-ticket".into(),
            GroupWebSocketTicket {
                email: "old@example.com".into(),
                expires_at: 99,
            },
        );

        assert_eq!(
            take_group_websocket_ticket(&mut tickets, "valid-ticket", 100).as_deref(),
            Some("user@example.com")
        );
        assert!(take_group_websocket_ticket(&mut tickets, "valid-ticket", 100).is_none());
        assert!(take_group_websocket_ticket(&mut tickets, "expired-ticket", 100).is_none());
    }

    #[test]
    fn validates_profile_photo_data_urls_and_accepts_legacy_users() {
        assert!(valid_avatar_data_url("data:image/png;base64,aGVsbG8="));
        assert!(valid_avatar_data_url("data:image/webp;base64,aGVsbG8="));
        assert!(!valid_avatar_data_url("data:image/svg+xml;base64,aGVsbG8="));
        assert!(!valid_avatar_data_url("data:image/png;base64,hello"));
        assert!(!valid_avatar_data_url("data:image/jpeg;base64,aGVs!G8="));

        let legacy_user: User = serde_json::from_str(
            r#"{"email":"user@example.com","nick":"User","password_hash":"hash","disabled":false}"#,
        )
        .unwrap();
        assert!(legacy_user.avatar_url.is_none());
    }

    #[test]
    fn deleting_group_channel_removes_its_empty_category() {
        let mut group = GroupModel {
            id: 1,
            name: "Test".into(),
            members_count: 1,
            owner_email: Some("owner@example.com".into()),
            moderator_emails: Vec::new(),
            member_emails: vec!["owner@example.com".into()],
            categories: vec![
                ChannelCategory {
                    id: "text".into(),
                    name: "TEXT".into(),
                    channels: vec![GroupChannel {
                        id: "text-one".into(),
                        name: "general".into(),
                        kind: "text".into(),
                    }],
                },
                ChannelCategory {
                    id: "voice".into(),
                    name: "VOICE".into(),
                    channels: vec![
                        GroupChannel {
                            id: "voice-one".into(),
                            name: "lobby".into(),
                            kind: "voice".into(),
                        },
                        GroupChannel {
                            id: "voice-two".into(),
                            name: "gaming".into(),
                            kind: "voice".into(),
                        },
                    ],
                },
            ],
        };

        assert!(remove_group_channel(&mut group, "voice-one"));
        assert_eq!(group.categories.len(), 2);
        assert_eq!(group.categories[1].channels.len(), 1);
        assert!(!remove_group_channel(&mut group, "missing"));
        assert!(remove_group_channel(&mut group, "text-one"));
        assert_eq!(group.categories.len(), 1);
        assert_eq!(group.categories[0].channels[0].id, "voice-two");
    }

    #[test]
    fn only_group_founder_can_delete_the_group() {
        let group = GroupModel {
            id: 7,
            name: "Test".into(),
            members_count: 2,
            owner_email: Some("owner@example.com".into()),
            moderator_emails: vec!["moderator@example.com".into()],
            member_emails: vec!["owner@example.com".into(), "moderator@example.com".into()],
            categories: Vec::new(),
        };

        assert!(can_delete_group(&group, "owner@example.com"));
        assert!(!can_delete_group(&group, "moderator@example.com"));
        assert!(!can_delete_group(&group, "member@example.com"));
    }

    #[test]
    fn expires_story_at_24_hour_boundary() {
        let story = StoryModel {
            id: "story".into(),
            author_email: "user@example.com".into(),
            author: "user".into(),
            text: "Hello".into(),
            image: None,
            video: None,
            created_at: 10_000,
            expires_at: 10_000 + 24 * 60 * 60,
        };

        assert!(!story_expired(&story, story.expires_at - 1));
        assert!(story_expired(&story, story.expires_at));
    }

    #[test]
    fn stories_are_visible_only_to_the_owner_and_friends() {
        let friends = HashSet::from(["friend@example.com".to_string()]);

        assert!(can_view_story(
            "owner@example.com",
            "owner@example.com",
            &friends
        ));
        assert!(can_view_story(
            "friend@example.com",
            "viewer@example.com",
            &friends
        ));
        assert!(!can_view_story(
            "stranger@example.com",
            "viewer@example.com",
            &friends
        ));
    }

    #[test]
    fn profile_followers_are_real_active_users_not_placeholder_counts() {
        let database = Database {
            users: vec![
                User {
                    email: "owner@example.com".into(),
                    nick: "Owner".into(),
                    password_hash: String::new(),
                    disabled: false,
                    avatar_url: None,
                },
                User {
                    email: "follower@example.com".into(),
                    nick: "Friend".into(),
                    password_hash: String::new(),
                    disabled: false,
                    avatar_url: None,
                },
                User {
                    email: "disabled@example.com".into(),
                    nick: "Disabled".into(),
                    password_hash: String::new(),
                    disabled: true,
                    avatar_url: None,
                },
            ],
            follows: vec![
                Follow {
                    follower_email: "follower@example.com".into(),
                    followed_email: "owner@example.com".into(),
                },
                Follow {
                    follower_email: "disabled@example.com".into(),
                    followed_email: "owner@example.com".into(),
                },
                Follow {
                    follower_email: "owner@example.com".into(),
                    followed_email: "follower@example.com".into(),
                },
            ],
            ..Database::default()
        };

        let profile =
            profile_social_response(&database, "follower@example.com", "owner@example.com", 3);
        assert_eq!(profile.followers, vec!["Friend"]);
        assert_eq!(profile.following, vec!["Friend"]);
        assert!(profile.is_following);
        assert_eq!(profile.posts, 3);
        let json = serde_json::to_string(&profile).unwrap();
        assert!(!json.contains("@example.com"));
        assert!(!json.contains("12"));
    }

    #[test]
    fn friend_presence_expires_after_ninety_seconds() {
        assert!(is_user_online(Some(&1_000), 1_090));
        assert!(!is_user_online(Some(&1_000), 1_091));
        assert!(!is_user_online(None, 1_000));
        assert!(!is_user_online(Some(&1_001), 1_000));
    }

    #[test]
    fn sessions_survive_database_serialization_and_legacy_data_loads() {
        let mut database = Database::default();
        database.sessions.insert(
            "persistent-session".into(),
            Session {
                email: "user@example.com".into(),
            },
        );

        let serialized = serde_json::to_string(&database).unwrap();
        let restored: Database = serde_json::from_str(&serialized).unwrap();
        assert_eq!(
            restored.sessions.get("persistent-session").unwrap().email,
            "user@example.com"
        );

        let legacy: Database = serde_json::from_str("{}").unwrap();
        assert!(legacy.sessions.is_empty());
        assert!(legacy.groups.is_empty());
    }

    #[test]
    fn persistent_session_cookie_is_read_without_overriding_a_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("theme=dark; mortex_session=persistent-token"),
        );
        assert_eq!(
            session_token_from_headers(&headers).as_deref(),
            Some("persistent-token")
        );

        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer bearer-token"),
        );
        assert_eq!(
            session_token_from_headers(&headers).as_deref(),
            Some("bearer-token")
        );
    }

    #[test]
    fn group_creator_and_moderator_permissions_are_server_side_and_persisted() {
        let group = GroupModel {
            id: 3,
            name: "Test".into(),
            members_count: 1,
            owner_email: Some("founder@example.com".into()),
            moderator_emails: vec!["mod@example.com".into()],
            member_emails: vec!["founder@example.com".into(), "mod@example.com".into()],
            categories: default_groups()[0].categories.clone(),
        };
        assert!(can_manage_group_channels(&group, "founder@example.com"));
        assert!(can_manage_group_channels(&group, "mod@example.com"));
        assert!(!can_manage_group_channels(&group, "member@example.com"));
        assert!(can_assign_group_moderators(&group, "founder@example.com"));
        assert!(!can_assign_group_moderators(&group, "mod@example.com"));

        let database = Database {
            groups: vec![group.clone()],
            ..Database::default()
        };
        let serialized = serde_json::to_string(&database).unwrap();
        let restored: Database = serde_json::from_str(&serialized).unwrap();
        assert_eq!(
            restored.groups[0].owner_email.as_deref(),
            Some("founder@example.com")
        );
        assert_eq!(
            restored.groups[0].categories[0].channels[0].name,
            "genel-sohbet"
        );

        let view = group_view(&group, "founder@example.com", &database);
        let public_json = serde_json::to_value(view).unwrap();
        assert_eq!(public_json["role"], "founder");
        assert!(public_json.get("owner_email").is_none());
        assert!(public_json.get("moderator_emails").is_none());
    }

    #[test]
    fn group_member_profiles_include_offline_members_and_online_status() {
        let group = GroupModel {
            id: 9,
            name: "Test".into(),
            members_count: 3,
            owner_email: Some("owner@example.com".into()),
            moderator_emails: vec!["moderator@example.com".into()],
            member_emails: vec![
                "owner@example.com".into(),
                "moderator@example.com".into(),
                "member@example.com".into(),
            ],
            categories: Vec::new(),
        };
        let users = ["owner", "moderator", "member", "not-a-member"]
            .into_iter()
            .map(|nick| User {
                email: format!("{nick}@example.com"),
                nick: nick.into(),
                password_hash: "hash".into(),
                disabled: false,
                avatar_url: None,
            })
            .collect::<Vec<_>>();

        let members = group_member_profiles(
            &group,
            &users,
            &HashSet::from(["moderator@example.com".into()]),
        );
        assert_eq!(members.len(), 3);
        let moderator = members
            .iter()
            .find(|member| member.nick == "moderator")
            .unwrap();
        assert_eq!(moderator.role, "Moderatör");
        assert!(moderator.online);
        let owner = members
            .iter()
            .find(|member| member.nick == "owner")
            .unwrap();
        assert_eq!(owner.role, "Kurucu");
        assert!(!owner.online);
        assert!(!members.iter().any(|member| member.nick == "not-a-member"));
    }

    #[test]
    fn group_channels_are_resolved_individually_for_channel_scoped_sockets() {
        let group = default_groups().remove(0);

        assert!(group_has_channel(&group, "general-chat"));
        assert!(group_has_channel(&group, "voice-room"));
        assert!(!group_has_channel(&group, "missing-channel"));
    }

    #[test]
    fn legacy_groups_load_without_new_role_or_category_fields() {
        let group: GroupModel =
            serde_json::from_str(r#"{"id":1,"name":"Legacy","members_count":4}"#).unwrap();
        assert!(group.owner_email.is_none());
        assert!(group.moderator_emails.is_empty());
        assert!(group.categories.is_empty());

        let mut legacy_database: Database =
            serde_json::from_str(r#"{"groups":[{"id":1,"name":"Legacy","members_count":4}]}"#)
                .unwrap();
        legacy_database.groups[0].categories = default_groups()[0].categories.clone();
        let upgraded: Database =
            serde_json::from_str(&serde_json::to_string(&legacy_database).unwrap()).unwrap();
        assert_eq!(upgraded.groups[0].categories[0].name, "GENEL");
    }

    #[test]
    fn direct_messages_require_friendship_and_keep_emails_out_of_api_responses() {
        let mut database = Database::default();
        database.friendships.push(Friendship {
            user_a: "alice@example.com".into(),
            user_b: "bob@example.com".into(),
        });
        assert!(are_friends(
            &database,
            "alice@example.com",
            "bob@example.com"
        ));
        assert!(!are_friends(
            &database,
            "alice@example.com",
            "stranger@example.com"
        ));

        let message = DirectMessage {
            id: "message-id".into(),
            from_email: "alice@example.com".into(),
            to_email: "bob@example.com".into(),
            from_nick: "Alice".into(),
            text: "hello".into(),
            created_at: 1,
        };
        database.direct_messages.push(message.clone());
        let stored = serde_json::to_value(&database).unwrap();
        assert_eq!(
            stored["direct_messages"][0]["from_email"],
            "alice@example.com"
        );
        let response = serde_json::to_value(DirectMessageResponse {
            id: message.id,
            from_nick: message.from_nick,
            text: message.text,
            created_at: message.created_at,
        })
        .unwrap();
        assert!(response.get("from_email").is_none());
        assert!(response.get("to_email").is_none());
    }

    #[test]
    fn direct_messages_survive_database_serialization() {
        let mut database = Database::default();
        database.direct_messages.push(DirectMessage {
            id: "message-id".into(),
            from_email: "alice@example.com".into(),
            to_email: "bob@example.com".into(),
            from_nick: "Alice".into(),
            text: "hello".into(),
            created_at: 1,
        });

        let serialized = serde_json::to_string(&database).unwrap();
        let restored: Database = serde_json::from_str(&serialized).unwrap();
        assert_eq!(restored.direct_messages.len(), 1);
        assert_eq!(restored.direct_messages[0].from_email, "alice@example.com");
        assert_eq!(restored.direct_messages[0].text, "hello");
    }
}
