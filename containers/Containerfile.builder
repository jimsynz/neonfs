FROM base

RUN mkdir /workdir
WORKDIR /workdir
COPY .tool-versions /workdir
RUN /usr/local/bin/asdf_install
RUN export PATH="/root/.asdf/shims:$PATH"; mix local.hex --force && mix local.rebar --force
