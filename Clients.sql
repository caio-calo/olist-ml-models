-- Databricks notebook source
WITH tb_entrega AS 
(
SELECT DISTINCT
  t1.idPedido,
  t1.idCliente,
  t2.idVendedor,
  t3.descUF
FROM silver.olist.pedido as t1

LEFT JOIN silver.olist.item_pedido as t2
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.cliente as t3
ON t1.idCliente=t3.idCliente

--intervalo de observação
WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01', -6)
)

SELECT DISTINCT idVendedor,
SUM(CASE WHEN descUF ='AC' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaAC,
SUM(CASE WHEN descUF ='AL' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaAL,
SUM(CASE WHEN descUF ='AM' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaAM,
SUM(CASE WHEN descUF ='AP' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaAP,
SUM(CASE WHEN descUF ='BA' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaBA,
SUM(CASE WHEN descUF ='CE' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaCE,
SUM(CASE WHEN descUF ='DF' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaDF,
SUM(CASE WHEN descUF ='ES' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaES,
SUM(CASE WHEN descUF ='GO' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaGO,
SUM(CASE WHEN descUF ='MA' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaMA,
SUM(CASE WHEN descUF ='MG' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaMG,
SUM(CASE WHEN descUF ='MS' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaMS,
SUM(CASE WHEN descUF ='MT' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaMT,
SUM(CASE WHEN descUF ='PA' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaPA,
SUM(CASE WHEN descUF ='PB' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaPB,
SUM(CASE WHEN descUF ='PE' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaPE,
SUM(CASE WHEN descUF ='PI' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaPI,
SUM(CASE WHEN descUF ='PR' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaPR,
SUM(CASE WHEN descUF ='RJ' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaRJ,
SUM(CASE WHEN descUF ='RN' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaRN,
SUM(CASE WHEN descUF ='RO' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaRO,
SUM(CASE WHEN descUF ='RR' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaRR,
SUM(CASE WHEN descUF ='RS' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaRS,
SUM(CASE WHEN descUF ='SC' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaSC,
SUM(CASE WHEN descUF ='SE' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaSE,
SUM(CASE WHEN descUF ='SP' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaSP,
SUM(CASE WHEN descUF ='TO' THEN '1' ELSE '0' END)/COUNT((descUF)) AS pctEntregaTO
FROM tb_entrega
GROUP BY idVendedor


-- COMMAND ----------


