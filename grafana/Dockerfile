# Eigenes Grafana-Image mit eingebackenem Provisioning (Datenquelle + Dashboards).
# Nötig, weil Bind-Mounts aus dem Git-Repo in Portainer/dind-Setups nicht ankommen.
FROM grafana/grafana:11.3.1

# Datenquelle + Dashboard-Provider (env-Variablen wie ${DB_PASSWORD} werden zur
# Laufzeit aus der Container-Umgebung expandiert).
COPY provisioning/ /etc/grafana/provisioning/

# Dashboards bewusst nach /etc/grafana/dashboards, NICHT unter /var/lib/grafana,
# da dort das Daten-Volume liegen und die Dateien verdecken würde.
COPY dashboards/ /etc/grafana/dashboards/
