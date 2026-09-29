# Custom Datadog Agent image with the official community "redpanda" check
# (datadog-redpanda, from integrations-extras) baked in. The stock agent
# image doesn't ship this check, and it can't be installed at runtime in a
# container - it has to be built in. This is what makes the "Redpanda
# Overview" out-of-the-box dashboard populate: that dashboard queries clean
# metric names like redpanda.cluster.brokers, which only this check produces
# (our generic openmetrics.d config produces differently-named metrics).
#
# See: https://github.com/DataDog/integrations-extras/tree/master/redpanda
FROM gcr.io/datadoghq/agent:7

ARG INTEGRATION_VERSION=2.2.0

RUN agent integration install -r -t datadog-redpanda==${INTEGRATION_VERSION}
