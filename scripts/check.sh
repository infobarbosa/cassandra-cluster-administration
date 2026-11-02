#!/usr/bin/env bash
#
# Verifica se o ambiente do laboratório de administração de cluster (6 nós,
# 2 datacenters x 3 racks) está íntegro. Roda no terminal do code-server:
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

cql() {
    for n in 1 2 3 4 5 6; do
        if cqlsh "127.0.0.${n}" -e "$1" 2>/dev/null; then
            return 0
        fi
    done
    return 1
}
valor() { cql "$1" | sed -n '4p' | tr -d ' '; }

titulo "Memória da Máquina"
MEM_TOTAL_MB=$(awk '/MemTotal/ {printf "%d", $2 / 1024}' /proc/meminfo 2>/dev/null || echo 0)
if [ "${MEM_TOTAL_MB}" -ge 7000 ]; then
    ok "Memória RAM detectada: ${MEM_TOTAL_MB} MB (compatível com o requisito de 8 GB)"
else
    aviso "Memória RAM detectada: ${MEM_TOTAL_MB} MB (recomendado: 8 GB / 8192 MB para os 6 nós)"
fi

titulo "Ferramentas"
for ferramenta in cqlsh nodetool nodetool1 nodetool2 nodetool3 nodetool4 nodetool5 nodetool6 node-start node-stop node-kill sstabledump; do
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

titulo "Inicialização do cluster e keyspaces"
if grep -q 'FALHOU\|ERRO:' /var/log/lab/carga.log 2>/dev/null; then
    falha "a inicialização do cluster falhou (veja /var/log/lab/carga.log)"
elif grep -q '^Keyspaces prontas' /var/log/lab/carga.log 2>/dev/null; then
    ok "6 nós iniciados em paralelo e keyspaces pré-carregadas prontas"
else
    aviso "subida dos 6 nós em andamento — acompanhe com: cat /var/log/lab/carga.log"
fi

titulo "Nós do cluster (datacenter1 e datacenter2)"
STATUS_OUT=""
for cmd in nodetool1 nodetool2 nodetool3 nodetool4 nodetool5 nodetool6; do
    STATUS_OUT=$("$cmd" status 2>/dev/null || true)
    [ -n "$STATUS_OUT" ] && break
done

if [ -z "$STATUS_OUT" ]; then
    falha "nenhum dos 6 nós respondeu ao nodetool (aguarde o bootstrap ou inicie um nó com node-start 1)"
    exit 1
fi

for n in 1 2 3 4 5 6; do
    ip="127.0.0.${n}"
    if [ "${n}" -le 3 ]; then
        dc="datacenter1"; rack="RAC${n}"
    else
        dc="datacenter2"; rack="RAC$((n - 3))"
    fi
    linha=$(echo "$STATUS_OUT" | grep -E "[[:space:]]${ip}[[:space:]]" || true)
    if echo "$linha" | grep -q "^UN" && echo "$linha" | grep -q "${rack}"; then
        ok "node${n} (${ip}, ${dc}, ${rack}): UN (Up/Normal)"
    elif [ -n "$linha" ]; then
        estado=$(echo "$linha" | awk '{print $1}')
        aviso "node${n} (${ip}, ${dc}, ${rack}): estado ${estado} (use 'node-start ${n}' se você o parou num exercício)"
    else
        falha "node${n} (${ip}, ${dc}, ${rack}) não aparece no anel"
    fi
done

VERSAO=$(valor "SELECT release_version FROM system.local;")
case "${VERSAO:-}" in
    5.*) ok "Cassandra versão $VERSAO" ;;
    "")  falha "nenhum nó respondeu ao cqlsh" ;;
    *)   aviso "versão $VERSAO — o laboratório foi escrito para a série 5.0" ;;
esac

titulo "Keyspaces e Replicação Multi-DC"
verifica_keyspace() {
    local keyspace="$1" esperadas="$2"
    local tabelas repl rf1 rf2
    tabelas=$(valor "SELECT count(*) FROM system_schema.tables WHERE keyspace_name='$keyspace';")
    repl=$(cql "DESCRIBE KEYSPACE $keyspace;" | head -n 5 || true)
    rf1=$(echo "$repl" | grep -o "'datacenter1': '[0-9]*'" | tr -dc '0-9' || true)
    rf2=$(echo "$repl" | grep -o "'datacenter2': '[0-9]*'" | tr -dc '0-9' || true)
    if [ -z "$tabelas" ] || [ "$tabelas" = "0" ]; then
        falha "$keyspace ausente"
        return
    fi
    if [ "$tabelas" -eq "$esperadas" ] && [ "${rf1:-}" = "3" ] && [ "${rf2:-}" = "3" ]; then
        ok "$keyspace: $tabelas tabelas (NetworkTopologyStrategy, datacenter1 = 3, datacenter2 = 3)"
    else
        aviso "$keyspace: $tabelas tabelas (esperadas $esperadas), datacenter1=${rf1:-?}, datacenter2=${rf2:-?}"
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
    printf '  \033[32mCluster de 6 nós pronto para o laboratório.\033[0m\n\n'
    exit 0
fi
printf '  \033[31m%d verificação(ões) falharam.\033[0m\n\n' "$FALHAS"
exit 1
