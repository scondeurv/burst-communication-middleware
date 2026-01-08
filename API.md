# Documentación de la API: burst-communication-middleware

Esta documentación describe las principales estructuras, enums, traits y funciones públicas de la librería `burst-communication-middleware`.

---

## 1. Configuración y Backends

### struct `Config`
Configura el middleware y los parámetros de backend.
- **Campos:**
  - `backend: Backend` — Backend de comunicación (S3, Redis, RabbitMQ...)
  - `server: Option<String>` — Dirección del servidor (si aplica)
  - `burst_id: String` — Identificador del burst
  - `burst_size: u32` — Número de workers
  - `group_ranges: HashMap<String, HashSet<u32>>` — Rango de grupos
  - `group_id: String` — ID del grupo
  - `chunking: bool` — Habilita particionado de mensajes
  - `chunk_size: usize` — Tamaño de chunk
  - `tokio_broadcast_channel_size: Option<usize>` — Tamaño del canal Tokio (opcional)

### enum `Backend`
Selecciona el backend de comunicación:
- `S3 { ... }`
- `RedisStream`
- `RedisList`
- `Rabbitmq`

---

## 2. Tipos de Mensaje y Metadatos

### struct `LocalMessage<T>`
Mensaje local con metadatos y datos tipados.
- `metadata: MessageMetadata`
- `data: T`

### struct `RemoteMessage`
Mensaje serializado para transporte remoto.
- `metadata: MessageMetadata`
- `data: Bytes`

### struct `MessageMetadata`
Metadatos de cada mensaje:
- `sender_id: u32`
- `chunk_id: u32`
- `num_chunks: u32`
- `counter: u32`
- `collective: CollectiveType`

### enum `CollectiveType`
Tipo de operación colectiva:
- `Direct`, `Broadcast`, `Scatter`, `Gather`, `AllToAll`

---

## 3. Middleware y Actor

### struct `Middleware<T>`
Instancia principal para operar con el middleware.
- **Métodos:**
  - `new(middleware: BurstMiddleware<T>, runtime: Handle) -> Self`
  - `get_actor_handle(self) -> MiddlewareActorHandle<T>`
  - `info(&self) -> BurstInfo`

### struct `MiddlewareActorHandle<T>`
Handle para operaciones síncronas de alto nivel.
- **Métodos:**
  - `send(&self, dest: u32, data: T) -> Result<()>`
  - `recv(&self, from: u32) -> Result<T>`
  - `broadcast(&self, data: Option<T>, root: u32) -> Result<T>`
  - `gather(&self, data: T, root: u32) -> Result<Option<Vec<T>>>`
  - `scatter(&self, data: Option<Vec<T>>, root: u32) -> Result<T>`
  - `all_to_all(&self, data: Vec<T>) -> Result<Vec<T>>`
  - `reduce(&self, data: T, op: fn(T, T) -> T) -> Result<Option<T>>`

---

## 4. Traits principales (para backends/extensión)

- `RemoteSendProxy`, `RemoteReceiveProxy`, `RemoteSendReceiveProxy`
- `LocalSendProxy<T>`, `LocalReceiveProxy<T>`, `LocalSendReceiveProxy<T>`
- `RemoteBroadcastSendProxy`, `RemoteBroadcastReceiveProxy`, `RemoteBroadcastProxy`
- `LocalBroadcastSendProxy<T>`, `LocalBroadcastReceiveProxy<T>`, `LocalBroadcastProxy<T>`
- `RemoteSendReceiveFactory<T>`, `SendReceiveLocalFactory<O, T>`

Estos traits definen la interfaz que deben implementar los distintos backends para integrarse con el middleware.

---

## 5. Funciones auxiliares

### `create_actors<T>(conf: Config, tokio_runtime: &Runtime) -> Result<HashMap<u32, Middleware<T>>>`
Crea y configura los actores para cada worker según el backend y la configuración.

---

## 6. Ejemplo de uso

```rust
let config = Config { /* ... */ };
let runtime = tokio::runtime::Runtime::new().unwrap();
let actors = create_actors::<Vec<u8>>(config, &runtime).unwrap();
let mw = actors.get(&0).unwrap();
let actor = mw.get_actor_handle();
actor.broadcast(Some(vec![1,2,3]), 0)?;
```

---

## 7. Notas
- Todos los métodos que involucran comunicación pueden devolver errores (`Result`).
- Los tipos y traits están pensados para ser genéricos y extensibles.
- Para añadir un backend, implementa los traits correspondientes y registra el backend en el enum `Backend`.

---

Para detalles adicionales, consulta los comentarios en el código fuente y los ejemplos en la carpeta `examples/`.
