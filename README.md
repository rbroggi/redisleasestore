# Redis lease store implementation for leaderelection Go Library

This library implements a store for the [leaderelection Go library](https://github.com/rbroggi/leaderelection) using Redis as the backend.

Compatible with Redis equal or above version 4.0.0.

## Installation

To install the `redisleasestore` library, use the following command:

```sh
go get github.com/rbroggi/redisleasestore
```

## Testing

To run the tests, use the following command:

```sh
go test ./...
```
## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

