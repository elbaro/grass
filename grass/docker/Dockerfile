FROM rustlang/rust:nightly-slim
ENV SHELL /bin/bash
RUN cargo install --git https://github.com/elbaro/grass.git
VOLUME ["/project"]
WORKDIR "/project"
ENTRYPOINT ["grass daemon --bind 127.0.0.1:7500 --no-worker"]
CMD ["/bin/bash"]
