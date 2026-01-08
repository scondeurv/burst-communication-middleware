# burst-communication-middleware

## Descripción general

`burst-communication-middleware` es una biblioteca escrita en Rust diseñada para facilitar la comunicación eficiente y flexible entre procesos o nodos en sistemas distribuidos. Su objetivo principal es abstraer y simplificar la implementación de patrones de comunicación como broadcast, reducción (reduce), envío de mensajes punto a punto y otros, sobre diferentes backends de transporte (Redis, S3, RabbitMQ, canales locales, etc.).

## Características principales

- **Abstracción de Middleware**: Proporciona una interfaz común para distintos backends de comunicación, permitiendo cambiar el mecanismo subyacente sin modificar la lógica de la aplicación.
- **Soporte para múltiples backends**: Incluye implementaciones para Redis (listas y streams), S3, RabbitMQ y canales Tokio, entre otros.
- **Patrones de comunicación**: Soporta operaciones como broadcast, reduce, gather, send/receive, útiles en aplicaciones de computación distribuida y paralela.
- **Ejemplos incluidos**: El directorio `examples/` contiene ejemplos prácticos de uso para distintos patrones y backends.
- **Extensible**: Es posible añadir nuevos backends implementando el trait correspondiente.

## Estructura del proyecto

- `src/` - Código fuente principal de la biblioteca.
  - `actor.rs` - Lógica de actores y manejo de mensajes.
  - `chunk_store.rs` - Utilidades para almacenamiento de fragmentos de datos.
  - `lib.rs` - Punto de entrada y definición de la API pública.
  - `message_buffer.rs` - Bufferización de mensajes para eficiencia y robustez.
  - `middleware.rs` - Definición del trait principal de middleware y lógica común.
  - `types.rs` - Definiciones de tipos y estructuras auxiliares.
  - `utils.rs` - Funciones utilitarias.
  - `backends/` - Implementaciones específicas de backends (Redis, S3, RabbitMQ, etc.).

- `examples/` - Ejemplos de uso:
  - `broadcast.rs` - Ejemplo de broadcast.
  - `reduce.rs` - Ejemplo de reducción.
  - Otros ejemplos para distintos patrones y backends.

- `Cargo.toml` - Archivo de configuración de Rust y dependencias.

## Casos de uso

- Algoritmos distribuidos (por ejemplo, propagación de etiquetas, reducción de resultados, etc.).
- Computación paralela y en clústeres.
- Sistemas de procesamiento de datos distribuidos.

## ¿Cómo funciona?

El usuario crea una instancia del middleware seleccionando el backend deseado y luego utiliza los métodos de la API para enviar, recibir, difundir o reducir mensajes entre los nodos participantes. La biblioteca se encarga de los detalles de serialización, sincronización y transporte de los mensajes.

## Ejemplo de uso básico

```rust
use burst_communication_middleware::{Middleware, MiddlewareConfig};

// Configuración del backend (por ejemplo, Redis)
let config = MiddlewareConfig::Redis { /* parámetros */ };
let mut middleware = Middleware::new(config);

// Enviar un mensaje de broadcast
data = vec![1, 2, 3];
middleware.broadcast(data, 0)?;

// Realizar una reducción (reduce)
let result = middleware.reduce(local_value, |a, b| a + b)?;
```

## Extensión y personalización

Para añadir un nuevo backend, basta con implementar el trait definido en `middleware.rs` y registrar el backend en el sistema. Esto permite adaptar la biblioteca a nuevas tecnologías de mensajería o almacenamiento distribuido.

## Licencia

La biblioteca está licenciada bajo la licencia MIT, lo que permite su uso libre en proyectos personales y comerciales.

---

Para más detalles, consulta el archivo `README.md` y los ejemplos incluidos en el repositorio.