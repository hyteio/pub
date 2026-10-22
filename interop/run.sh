#!/usr/bin/env bash
# Real-broker test suite for pub (opt-in; needs Docker, a JDK and openssl).
#
# Starts ActiveMQ Classic and Artemis (OpenWire plain / TLS / mutual TLS, STOMP,
# STOMP over TLS, MQTT, AMQP 1.0, Classic's AMQP over TLS), RabbitMQ 4 (AMQP
# 1.0) and Mosquitto (MQTT plain / TLS / password), compiles
# the Java interop helpers against Classic's own client jars, then runs the
# ignored tests in cli/tests/brokers.rs against them. Everything is torn down
# afterwards.
#
#   interop/run.sh                 # all broker tests
#   interop/run.sh tls             # only tests whose name contains "tls"
#   interop/run.sh durable --nocapture   # extra arguments go to the test runner
#   KEEP=1 interop/run.sh          # leave the brokers running afterwards
#   AZURE_SERVICEBUS=1 interop/run.sh  # also the Azure Service Bus emulator (see below)
#
# Azure Service Bus: Microsoft's emulator (with SQL Server Linux for its state)
# is opt-in. AZURE_SERVICEBUS=1 accepts the emulator's and SQL Server's license terms
# (https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt,
# https://go.microsoft.com/fwlink/?LinkId=746388). Its entities are fixed at
# start-up by interop/servicebus/Config.json. SQL Server is amd64-only, so on
# arm64 it runs emulated and the first start takes a few minutes.
#
# Images: ACTIVEMQ_IMAGE (apache/activemq:6.3.2), ARTEMIS_IMAGE
# (apache/activemq-artemis:latest), MOSQUITTO_IMAGE (eclipse-mosquitto:2),
# RABBITMQ_IMAGE (rabbitmq:4-management), AZURE_SERVICEBUS_IMAGE
# (mcr.microsoft.com/azure-messaging/servicebus-emulator:latest), AZURE_MSSQL_IMAGE
# (mcr.microsoft.com/mssql/server:2022-latest).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
work="$(mktemp -d "${TMPDIR:-/tmp}/pub-interop.XXXXXX")"
classic_image="${ACTIVEMQ_IMAGE:-apache/activemq:6.3.2}"
artemis_image="${ARTEMIS_IMAGE:-apache/activemq-artemis:latest}"
mosquitto_image="${MOSQUITTO_IMAGE:-eclipse-mosquitto:2}"
rabbitmq_image="${RABBITMQ_IMAGE:-rabbitmq:4-management}"
servicebus_image="${AZURE_SERVICEBUS_IMAGE:-mcr.microsoft.com/azure-messaging/servicebus-emulator:latest}"
mssql_image="${AZURE_MSSQL_IMAGE:-mcr.microsoft.com/mssql/server:2022-latest}"
pw=changeit
# Host ports: OpenWire plain / TLS / mTLS, STOMP.
CLASSIC_PORT=${CLASSIC_PORT:-61716} CLASSIC_TLS_PORT=${CLASSIC_TLS_PORT:-61717}
CLASSIC_MTLS_PORT=${CLASSIC_MTLS_PORT:-61718} CLASSIC_STOMP_PORT=${CLASSIC_STOMP_PORT:-61713}
CLASSIC_STOMP_TLS_PORT=${CLASSIC_STOMP_TLS_PORT:-61712} CLASSIC_MQTT_PORT=${CLASSIC_MQTT_PORT:-61783}
ARTEMIS_PORT=${ARTEMIS_PORT:-61726} ARTEMIS_TLS_PORT=${ARTEMIS_TLS_PORT:-61727}
ARTEMIS_STOMP_PORT=${ARTEMIS_STOMP_PORT:-61723} ARTEMIS_MQTT_PORT=${ARTEMIS_MQTT_PORT:-61793}
MOSQUITTO_PORT=${MOSQUITTO_PORT:-61883} MOSQUITTO_TLS_PORT=${MOSQUITTO_TLS_PORT:-61884}
MOSQUITTO_AUTH_PORT=${MOSQUITTO_AUTH_PORT:-61885}
CLASSIC_AMQP_PORT=${CLASSIC_AMQP_PORT:-61772} CLASSIC_AMQPS_PORT=${CLASSIC_AMQPS_PORT:-61771}
ARTEMIS_AMQP_PORT=${ARTEMIS_AMQP_PORT:-61773} RABBITMQ_AMQP_PORT=${RABBITMQ_AMQP_PORT:-61774}
AZURE_SERVICEBUS_PORT=${AZURE_SERVICEBUS_PORT:-61775} AZURE_SERVICEBUS_HTTP_PORT=${AZURE_SERVICEBUS_HTTP_PORT:-61776}
containers="pub-it-classic pub-it-artemis pub-it-mosquitto pub-it-rabbitmq pub-it-servicebus pub-it-sb-sql"

for tool in docker java javac keytool openssl curl; do
  command -v "$tool" >/dev/null || { echo "interop: $tool is required" >&2; exit 1; }
done

cleanup() {
  if [ -z "${KEEP:-}" ]; then
    docker rm -f $containers >/dev/null 2>&1 || true
    docker network rm pub-it-sb >/dev/null 2>&1 || true
    rm -rf "$work"
  else
    echo "interop: brokers left running; files in $work"
  fi
}
trap cleanup EXIT

echo "interop: certificates"
certs="$work/certs"; mkdir -p "$certs"; cd "$certs"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
  -subj "/CN=pub interop CA" -keyout ca.key -out ca.pem 2>/dev/null
issue() { # name, subject, extensions
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "$2" \
    -keyout "$1.key" -out "$1.csr" 2>/dev/null
  printf '%s\n' "$3" > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 \
    -extfile "$1.ext" -out "$1.pem" 2>/dev/null
}
issue server "/CN=localhost" "subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth"
issue client "/CN=pub interop client" "extendedKeyUsage=clientAuth"
openssl pkcs12 -export -in server.pem -inkey server.key -certfile ca.pem \
  -name server -passout pass:$pw -out server.p12
keytool -importcert -noprompt -alias ca -file ca.pem -keystore trust.p12 \
  -storetype PKCS12 -storepass $pw >/dev/null 2>&1
# pub's key-file checks want the key private to its owner.
chmod 600 client.key ca.key server.key
# Readable by the brokers' (non-root) users inside the containers.
chmod 644 server.p12 trust.p12
chmod 755 "$certs"

echo "interop: broker configuration"
mkdir -p "$work/classic" "$work/artemis"
docker run --rm --entrypoint cat "$classic_image" /opt/apache-activemq/conf/activemq.xml \
  > "$work/classic/activemq.xml"
python3 - "$work/classic/activemq.xml" <<PY
import re, sys
p = sys.argv[1]; s = open(p).read()
s = s.replace('<broker xmlns="http://activemq.apache.org/schema/core"',
              '<broker xmlns="http://activemq.apache.org/schema/core" schedulerSupport="true"', 1)
s = s.replace('<transportConnectors>', '''<sslContext>
            <sslContext keyStore="file:/certs/server.p12" keyStorePassword="$pw" keyStoreType="PKCS12"
                        trustStore="file:/certs/trust.p12" trustStorePassword="$pw" trustStoreType="PKCS12"/>
        </sslContext>
        <transportConnectors>
            <transportConnector name="pub-tls" uri="ssl://0.0.0.0:61617"/>
            <transportConnector name="pub-mtls" uri="ssl://0.0.0.0:61618?needClientAuth=true"/>
            <transportConnector name="pub-stomp-tls" uri="stomp+ssl://0.0.0.0:61612"/>
            <transportConnector name="pub-amqp-tls" uri="amqp+ssl://0.0.0.0:5671?transport.transformer=jms"/>''', 1)
# ActiveMQ 6.3+ ships with only OpenWire enabled: turn the AMQP, STOMP and MQTT
# connectors back on (a no-op where they already are).
s = re.sub(r'<!--\s*(<transportConnector name="(?:amqp|stomp|mqtt)"[^>]*/>)\s*-->', r'\1', s)
# ActiveMQ's AMQP connector defaults to the *native* transformer (JMS consumers
# would see raw AMQP bytes); JMS mapping is what interop wants.
s, n = re.subn(r'(uri="amqp://0\.0\.0\.0:5672\?)', r'\1transport.transformer=jms&amp;', s)
assert n == 1, "no amqp connector in activemq.xml"
open(p, "w").write(s)
PY
docker run --rm --entrypoint sh "$artemis_image" -c \
  '/opt/activemq-artemis/bin/artemis create --user x --password x --silent --require-login /tmp/i >/dev/null && cat /tmp/i/etc/broker.xml' \
  > "$work/artemis/broker.xml"
python3 - "$work/artemis/broker.xml" <<PY
import sys
p = sys.argv[1]; s = open(p).read()
s = s.replace('<acceptors>', '''<acceptors>
         <acceptor name="pub-tls">tcp://0.0.0.0:61617?sslEnabled=true;keyStorePath=/certs/server.p12;keyStorePassword=$pw;keyStoreType=PKCS12;protocols=OPENWIRE,CORE</acceptor>''', 1)
# STOMP's /queue/ and /topic/ mean what they mean on ActiveMQ.
import re
s, n = re.subn(r'(<acceptor name="stomp">[^<]*)(</acceptor>)', r'\1;anycastPrefix=/queue/;multicastPrefix=/topic/\2', s)
assert n == 1, "no stomp acceptor in broker.xml"
open(p, "w").write(s)
PY

mkdir -p "$work/mosquitto"
# The broker runs as its own user: give it readable copies of the test key and certs.
cp "$certs/ca.pem" "$certs/server.pem" "$work/mosquitto/"
cp "$certs/server.key" "$work/mosquitto/server.key"; chmod 644 "$work/mosquitto/server.key"
cat > "$work/mosquitto/mosquitto.conf" <<CONF
per_listener_settings true
listener 1883
allow_anonymous true
listener 8883
allow_anonymous true
cafile /mosquitto/config/ca.pem
certfile /mosquitto/config/server.pem
keyfile /mosquitto/config/server.key
listener 1884
allow_anonymous false
password_file /mosquitto/config/passwd
CONF
docker run --rm -v "$work/mosquitto:/m" "$mosquitto_image" mosquitto_passwd -b -c /m/passwd app s3cret
chmod 644 "$work/mosquitto/passwd" 2>/dev/null || true

echo "interop: starting brokers"
docker rm -f $containers >/dev/null 2>&1 || true
docker run -d --name pub-it-classic \
  -p $CLASSIC_PORT:61616 -p $CLASSIC_TLS_PORT:61617 -p $CLASSIC_MTLS_PORT:61618 -p $CLASSIC_STOMP_PORT:61613 \
  -p $CLASSIC_STOMP_TLS_PORT:61612 -p $CLASSIC_MQTT_PORT:1883 \
  -p $CLASSIC_AMQP_PORT:5672 -p $CLASSIC_AMQPS_PORT:5671 \
  -v "$certs:/certs:ro" -v "$work/classic/activemq.xml:/opt/apache-activemq/conf/activemq.xml:ro" \
  "$classic_image" >/dev/null
docker run -d --name pub-it-artemis -p $ARTEMIS_PORT:61616 -p $ARTEMIS_TLS_PORT:61617 \
  -p $ARTEMIS_STOMP_PORT:61613 -p $ARTEMIS_MQTT_PORT:1883 -p $ARTEMIS_AMQP_PORT:5672 \
  -e ARTEMIS_USER=artemis -e ARTEMIS_PASSWORD=artemis \
  -v "$certs:/certs:ro" -v "$work/artemis:/var/lib/artemis-instance/etc-override:ro" \
  "$artemis_image" >/dev/null
docker run -d --name pub-it-mosquitto -p $MOSQUITTO_PORT:1883 -p $MOSQUITTO_TLS_PORT:8883 \
  -p $MOSQUITTO_AUTH_PORT:1884 -v "$work/mosquitto:/mosquitto/config" "$mosquitto_image" >/dev/null
docker run -d --name pub-it-rabbitmq -p $RABBITMQ_AMQP_PORT:5672 "$rabbitmq_image" >/dev/null
if [ -n "${AZURE_SERVICEBUS:-}" ]; then
  # The SA password only lives inside this throwaway network.
  sql_pw="Pub-interop-$RANDOM-x1"
  docker network create pub-it-sb >/dev/null 2>&1 || true
  docker run -d --name pub-it-sb-sql --network pub-it-sb --network-alias mssql --platform linux/amd64 \
    -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$sql_pw" "$mssql_image" >/dev/null
  docker run -d --name pub-it-servicebus --network pub-it-sb \
    -p $AZURE_SERVICEBUS_PORT:5672 -p $AZURE_SERVICEBUS_HTTP_PORT:5300 \
    -v "$here/servicebus/Config.json:/ServiceBus_Emulator/ConfigFiles/Config.json:ro" \
    -e SQL_SERVER=mssql -e "MSSQL_SA_PASSWORD=$sql_pw" -e ACCEPT_EULA=Y "$servicebus_image" >/dev/null
fi
wait_for() { # container, log line
  for _ in $(seq 1 60); do
    docker logs "$1" 2>&1 | grep -q "$2" && return 0
    sleep 1
  done
  echo "interop: $1 did not start:" >&2; docker logs "$1" 2>&1 | tail -30 >&2; exit 1
}
wait_for pub-it-classic "Apache ActiveMQ .* started"
wait_for pub-it-artemis "AMQ221007"
wait_for pub-it-mosquitto "running"
wait_for pub-it-rabbitmq "Server startup complete"
if [ -n "${AZURE_SERVICEBUS:-}" ]; then
  echo "interop: waiting for the Service Bus emulator"
  for _ in $(seq 1 200); do
    curl -sf "http://localhost:$AZURE_SERVICEBUS_HTTP_PORT/health" >/dev/null && break
    sleep 3
  done
  curl -sf "http://localhost:$AZURE_SERVICEBUS_HTTP_PORT/health" >/dev/null || {
    echo "interop: the Service Bus emulator did not start:" >&2
    docker logs pub-it-servicebus 2>&1 | tail -30 >&2; exit 1; }
  # The emulator's fixed key (it accepts any, in fact).
  export PUB_IT_AZURE_SERVICEBUS="amqp://RootManageSharedAccessKey:SAS_KEY_VALUE@localhost:$AZURE_SERVICEBUS_PORT"
fi

echo "interop: Java helpers"
docker cp pub-it-classic:/opt/apache-activemq/lib "$work/lib" >/dev/null
mkdir -p "$work/classes"
javac -nowarn -cp "$work/lib/*:$work/lib/optional/*" -d "$work/classes" "$here"/java/*.java

export PUB_IT_CLASSIC="amq://localhost:$CLASSIC_PORT"
export PUB_IT_CLASSIC_TLS="amqs://localhost:$CLASSIC_TLS_PORT"
export PUB_IT_CLASSIC_MTLS="amqs://localhost:$CLASSIC_MTLS_PORT"
export PUB_IT_CLASSIC_STOMP="stomp://localhost:$CLASSIC_STOMP_PORT"
export PUB_IT_CLASSIC_JAVA="tcp://localhost:$CLASSIC_PORT"
export PUB_IT_CLASSIC_STOMP_TLS="stomps://localhost:$CLASSIC_STOMP_TLS_PORT"
export PUB_IT_CLASSIC_MQTT="mqtt://localhost:$CLASSIC_MQTT_PORT"
export PUB_IT_ARTEMIS_STOMP="stomp://artemis:artemis@localhost:$ARTEMIS_STOMP_PORT"
export PUB_IT_ARTEMIS_MQTT="mqtt://artemis:artemis@localhost:$ARTEMIS_MQTT_PORT"
export PUB_IT_MOSQUITTO="mqtt://localhost:$MOSQUITTO_PORT"
export PUB_IT_MOSQUITTO_TLS="mqtts://localhost:$MOSQUITTO_TLS_PORT"
export PUB_IT_MOSQUITTO_AUTH="mqtt://localhost:$MOSQUITTO_AUTH_PORT"
export PUB_IT_CLASSIC_AMQP="amqp://localhost:$CLASSIC_AMQP_PORT"
export PUB_IT_CLASSIC_AMQPS="amqps://localhost:$CLASSIC_AMQPS_PORT"
export PUB_IT_ARTEMIS_AMQP="amqp://artemis:artemis@localhost:$ARTEMIS_AMQP_PORT"
export PUB_IT_RABBITMQ_AMQP="amqp://guest:guest@localhost:$RABBITMQ_AMQP_PORT"
# RabbitMQ serves AMQP 0-9-1 and 1.0 on the same port.
export PUB_IT_RABBITMQ_RAMPQ="rampq://guest:guest@localhost:$RABBITMQ_AMQP_PORT"
# RabbitMQ needs its queues declared; tests do it through this container.
export PUB_IT_RABBITMQ_CONTAINER=pub-it-rabbitmq
export PUB_IT_ARTEMIS="amq://artemis:artemis@localhost:$ARTEMIS_PORT"
export PUB_IT_ARTEMIS_TLS="amqs://artemis:artemis@localhost:$ARTEMIS_TLS_PORT"
export PUB_IT_ARTEMIS_JAVA="tcp://localhost:$ARTEMIS_PORT"
export PUB_IT_CA="$certs/ca.pem" PUB_IT_CLIENT_CERT="$certs/client.pem" PUB_IT_CLIENT_KEY="$certs/client.key"
export PUB_IT_JAVA_CP="$work/lib/*:$work/lib/optional/*:$work/classes"

echo "interop: running tests"
cd "$root"
cargo test -p pub-cli --test brokers -- --ignored --test-threads=4 "$@"
