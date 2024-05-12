# Begin the proper packaging of the image to run the binary.
FROM ubuntu:22.04

ENV TZ UTC
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV PATH $PATH:/usr/local/bin:/usr/local/go/bin
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -qqy update \
  && apt-get install -y locales \
  && sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
  && dpkg-reconfigure locales && update-locale LANG=en_US.UTF-8 \
  && apt-get -qqy install \
    ca-certificates \
    tzdata \
    curl \
  && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone \
  && dpkg-reconfigure tzdata \
  && apt-get -qyy autoremove \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get -qyy clean \
  && mkdir /usr/share/ca-certificates/extra

RUN find /etc/ssl/certs -type f -exec chmod 0644 {} \; && \
  find /usr/share/ca-certificates -type f -exec chmod 0644 {} \; && \
  find /usr/share/ca-certificates -type f -printf "%P\n" > /etc/ca-certificates.conf && \
  dpkg-reconfigure ca-certificates && update-ca-certificates

RUN echo $PATH
RUN groupadd --gid 1337 "ping42" \
	&& useradd --uid 1337 --gid 1337 --home "/go" "ping42"

WORKDIR /go

COPY scheduler ./scheduler

# Set permissions on app dir and clenup /tmp/*.
RUN chown ping42: -R ./ && chmod go-w -R ./ && rm -rf /tmp/*

USER ping42

CMD ["./scheduler"]