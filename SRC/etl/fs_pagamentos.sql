-- Databricks notebook source
WITH tb_join AS (
SELECT 
      t2.*,
      t3.idVendedor 

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.pagamento_pedido AS t2
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.item_pedido AS t3
ON t1.idPedido = t3.idPedido

--intervalo de observação
WHERE 
      t1.dtPedido < '2018-01-01'
      AND t1.dtPedido >= add_months('2018-01-01', -6)
),

tb_group AS (select idVendedor,
       descTipoPagamento,
       count(distinct descTipoPagamento) AS qtPedidoMeioPagamento,
       sum(vlPagamento) AS PagamentoTotal
FROM tb_join

GROUP BY 1,2 
ORDER BY 1 DESC),

tb_pagamentos AS ( 
SELECT idVendedor,

-- Volume de pedidos
sum(CASE WHEN descTipoPagamento = 'boleto' then qtPedidoMeioPagamento else 0 end) as qtBoleto,
sum(CASE WHEN descTipoPagamento = 'credit_card' then qtPedidoMeioPagamento else 0 end) as qtCartao,
sum(CASE WHEN descTipoPagamento = 'voucher' then qtPedidoMeioPagamento else 0 end) as qtVoucher,
sum(CASE WHEN descTipoPagamento = 'debit_card' then qtPedidoMeioPagamento else 0 end) as qtDebito,

sum(CASE WHEN descTipoPagamento = 'boleto' then qtPedidoMeioPagamento else 0 end)/sum(qtPedidoMeioPagamento) as prop_qtBoleto,
sum(CASE WHEN descTipoPagamento = 'credit_card' then qtPedidoMeioPagamento else 0 end)/sum(qtPedidoMeioPagamento) as prop_qtCartao,
sum(CASE WHEN descTipoPagamento = 'voucher' then qtPedidoMeioPagamento else 0 end)/sum(qtPedidoMeioPagamento) as prop_qtVoucher,
sum(CASE WHEN descTipoPagamento = 'debit_card' then qtPedidoMeioPagamento else 0 end)/sum(qtPedidoMeioPagamento) as prop_qtDebito,

--Valor dos pedidos
sum(CASE WHEN descTipoPagamento = 'boleto' then PagamentoTotal else 0 end) as vlBoleto,
sum(CASE WHEN descTipoPagamento = 'credit_card' then PagamentoTotal else 0 end) as vlCartao,
sum(CASE WHEN descTipoPagamento = 'voucher' then PagamentoTotal else 0 end) as vlVoucher,
sum(CASE WHEN descTipoPagamento = 'debit_card' then PagamentoTotal else 0 end) as vlDebito,

sum(CASE WHEN descTipoPagamento = 'boleto' then PagamentoTotal else 0 end)/sum(PagamentoTotal) as prop_vlBoleto,
sum(CASE WHEN descTipoPagamento = 'credit_card' then PagamentoTotal else 0 end)/sum(PagamentoTotal) as prop_vlCartao,
sum(CASE WHEN descTipoPagamento = 'voucher' then PagamentoTotal else 0 end)/sum(PagamentoTotal) as prop_vlVoucher,
sum(CASE WHEN descTipoPagamento = 'debit_card' then PagamentoTotal else 0 end)/sum(PagamentoTotal) as prop_vlDebito



FROM tb_group
GROUP BY 1
),

--Variáveis de crédito
tb_cartao AS (
SELECT 
      idVendedor, 
      avg(nrParcelas) AS avgNrParcelas, 
      percentile(nrParcelas, 0.5 ) as medianNrParcelas, 
      min(nrParcelas) AS minNrParcelas, 
      max(nrParcelas) AS maxNrParcelas 
FROM tb_join
WHERE descTipoPagamento = 'credit_card'
GROUP BY idVendedor
)

--Tabela final com referência
SELECT '2018-01-01' as dtRef,
      t1.*,
      t2.avgNrParcelas,
      t2.medianNrParcelas,
      t2.minnrParcelas,
      t2.maxNrParcelas
FROM tb_pagamentos as t1

LEFT JOIN tb_cartao as t2
ON t1.idVendedor = t2.idVendedor


