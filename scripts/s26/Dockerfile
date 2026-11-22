FROM debian:12.9-slim
ENV container=docker
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    systemd systemd-sysv dbus dbus-user-session libpam-systemd openssh-server git ca-certificates \
    bash coreutils util-linux procps libgcc-s1 sqlite3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 --shell /bin/bash belt \
    && passwd -d belt \
    && mkdir -p /var/lib/systemd/linger /run/sshd /home/belt/.ssh \
    && touch /var/lib/systemd/linger/belt \
    && chmod 700 /home/belt/.ssh \
    && chown -R belt:belt /home/belt/.ssh \
    && printf '%s\n' 'PasswordAuthentication no' 'KbdInteractiveAuthentication no' \
       'PermitRootLogin no' 'AllowUsers belt' 'UsePAM yes' > /etc/ssh/sshd_config.d/s26.conf \
    && systemctl enable ssh \
    && rm -f /etc/ssh/ssh_host_* /etc/machine-id \
    && touch /etc/machine-id
COPY worker-entrypoint.sh /usr/local/sbin/s26-entrypoint
STOPSIGNAL SIGRTMIN+3
ENTRYPOINT ["/usr/local/sbin/s26-entrypoint"]
