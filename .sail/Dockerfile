FROM codercom/ubuntu-dev-openjdk12

RUN sudo apt-get update && sudo apt-get install maven bash-completion

# complete -W '$(just --summary)' just
RUN curl -LSfs https://japaric.github.io/trust/install.sh | sudo sh -s -- --git casey/just --target x86_64-unknown-linux-musl --to /usr/local/bin
RUN echo "complete -W '$(just --summary)' just" >> ~/.bashrc

RUN installext redhat.java
RUN installext k--kato.intellij-idea-keybindings

