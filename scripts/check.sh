#!/usr/bin/env bash
#
# Verifica se o ambiente do laboratório de administração de cluster está íntegro.
# Roda no terminal do code-server:
#
#   curl -sS https://raw.githubusercontent.com/infobarbosa/cassandra-cluster-administration/main/scripts/check.sh | bash
#
# Use este script sempre que uma seção do laboratório não se comportar como o
# texto descreve: ele diz o que está faltando em vez de deixar você adivinhar.
#
set -uo pipefail

FALHAS=0

ok()    { printf '  \033[32m✓\033[0m %s\n' "$1"; }
falha() { printf '  \033[31m✗\033[0m %s\n' "$1"; FALHAS=$((FALHAS + 1)); }
aviso() { printf '  \033[33m!\033[0m %s\n' "$1"; }
titulo(){ printf '\n\033[1m%s\033[0m\n' "$1"; }

cql() { cqlsh 127.0.0.1 -e "$1" 2>/dev/null || cqlsh 127.0.0.2 -e "$1" 2>/dev/null || cqlsh 127.0.0.3 -e "$1" 2>/dev/null; }
valor() { cql "$1" | sed -n '4p' | tr -d ' '; }

titulo "Ferramentas"
for ferramenta in cqlsh nodetool nodetool1 nodetool2 nodetool3 node-start node-stop node-kill sstabledump; do
    if command -v "$ferramenta" >/dev/null 2>&1; then
        ok "$ferramenta"
    else
        falha "$ferramenta não encontrado no PATH"
    fi
done
if ! command -v cqlsh >/dev/null 2>&1; then
    echo; echo "Este script roda no terminal do code-server, não no CloudShell nem no seu computador."
    exit 1
fi

titulo "Carga inicial do cluster"
if grep -q 'FALHOU' /var/log/lab/carga.log 2>/dev/null; then
    falha "a carga das keyspaces falhou (veja /var/log/lab/carga.log)"
elif [ -f /var/lib/cassandra/.keyspaces-carregadas ] || grep -q '^Keyspaces prontas' /var/log/lab/carga.log 2>/dev/null; then
    ok "bootstrap dos 3 nós e carga do primeiro boot concluídos"
else
    aviso "bootstrap/carga em andamento — acompanhe com: cat /var/log/lab/carga.log"
fi

titulo "Nós do cluster (datacenter1)"
STATUS_OUT=$(nodetool1 status 2>/dev/null || nodetool2 status 2>/dev/null || nodetool3 status 2>/dev/null || true)
if [ -z "$STATUS_OUT" ]; then
    falha "nenhum dos 3 nós respondeu ao nodetool (aguarde o bootstrap ou inicie um nó com node-start 1)"
    exit 1
fi

for n in 1 2 3; do
    ip="127.0.0.${n}"
    rack="RAC${n}"
    linha=$(echo "$STATUS_OUT" | grep -E "[[:space:]]${ip}[[:space:]]" || true)
    if echo "$linha" | grep -q "^UN" && echo "$linha" | grep -q "${rack}"; then
        ok "node${n} (${ip}, ${rack}): UN (Up/Normal)"
    elif [ -n "$linha" ]; then
        estado=$(echo "$linha" | awk '{print $1}')
        aviso "node${n} (${ip}, ${rack}): estado ${estado} (use 'node-start ${n}' se você o parou num exercício)"
    else
        falha "node${n} (${ip}, ${rack}) não aparece no anel"
    fi
done

VERSAO=$(valor "SELECT release_version FROM system.local;")
case "${VERSAO:-}" in
    5.*) ok "Cassandra versão $VERSAO" ;;
    "")  falha "nenhum nó respondeu ao cqlsh" ;;
    *)   aviso "versão $VERSAO — o laboratório foi escrito para a série 5.0" ;;
esac

titulo "Keyspaces e Replicação"
verifica_keyspace() {
    local keyspace="$1" esperadas="$2"
    local tabelas rf
    tabelas=$(valor "SELECT count(*) FROM system_schema.tables WHERE keyspace_name='$keyspace';")
    rf=$(cql "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name='$keyspace';" | grep -o "'datacenter1': '[0-9]*'" | grep -o "[0-9]*$" || true)
    if [ -z "$tabelas" ] || [ "$tabelas" = "0" ]; then
        falha "$keyspace ausente"
        return
    fi
    if [ "$tabelas" -eq "$esperadas" ] && [ "${rf:-}" = "3" ]; then
        ok "$keyspace: $tabelas tabelas (NetworkTopologyStrategy, datacenter1 = 3)"
    else
        aviso "$keyspace: $tabelas tabelas (esperadas $esperadas), RF=${rf:-?}"
    fi
}

verifica_keyspace infobarbank 12
verifica_keyspace infobarshop  9

titulo "Dados"
CLIENTES=$(valor "SELECT count(*) FROM infobarbank.cliente;")
[ "${CLIENTES:-0}" -gt 0 ] && ok "infobarbank.cliente: $CLIENTES clientes" || falha "infobarbank.cliente vazia"

PEDIDOS=$(valor "SELECT count(*) FROM infobarshop.pedido_detalhe;")
[ "${PEDIDOS:-0}" -gt 0 ] && ok "infobarshop.pedido_detalhe: $PEDIDOS pedidos" || falha "infobarshop.pedido_detalhe vazia"

titulo "Resultado"
if [ "$FALHAS" -eq 0 ]; then
    printf '  \033[32mCluster pronto para o laboratório.\033[0m\n\n'
    exit 0
fi
printf '  \033[31m%d verificação(ões) falharam.\033[0m\n\n' "$FALHAS"
exit 1
