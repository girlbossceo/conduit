# Setting up TURN/STURN

In order to make or receive calls, a TURN server is required. conduwuit suggests using [Coturn](https://github.com/coturn/coturn) for this purpose, which is also available as a Docker image.

### Configuration

Create a configuration file called `coturn.conf` containing:

```conf
use-auth-secret
static-auth-secret=<a secret key>
realm=<your server domain>
```
A common way to generate a suitable alphanumeric secret key is by using `pwgen -s 64 1`.

These same values need to be set in conduwuit. See the [example config](configuration/examples.md) in the TURN section for configuring these and restart conduwuit after.

### Run
Run the [Coturn](https://hub.docker.com/r/coturn/coturn) image using
```bash
docker run -d --network=host -v $(pwd)/coturn.conf:/etc/coturn/turnserver.conf coturn/coturn
```

or docker-compose. For the latter, paste the following section into a file called `docker-compose.yml`
and run `docker compose up -d` in the same directory.

```yml
version: 3
services:
    turn:
      container_name: coturn-server
      image: docker.io/coturn/coturn
      restart: unless-stopped
      network_mode: "host"
      volumes:
        - ./coturn.conf:/etc/coturn/turnserver.conf
```

To understand why the host networking mode is used and explore alternative configuration options, please visit [Coturn's Docker documentation](https://github.com/coturn/coturn/blob/master/docker/coturn/README.md).

For security recommendations see Synapse's [Coturn documentation](https://element-hq.github.io/synapse/latest/turn-howto.html).
