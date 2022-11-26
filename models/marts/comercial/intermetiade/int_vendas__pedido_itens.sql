with
    ordens as (
        select
            id_pedido
            , id_funcionario
            , id_cliente
            , id_transportadora
            , data_do_pedido
            , frete
            , destinatario
            , endereco_destinatario
            , cep_destinatario
            , cidade_destinatario
            , regiao_destinatario
            , pais_destinatario
            , data_do_envio
            , data_requerida
        from {{ ref('stg_erp__ordens') }}
    )

    , ordem_detalhes as (
        select
           id_pedido 
            , id_produto
            , desconto
            , preco_da_unidade
            , quantidade
        from {{ ref('stg_erp__ordem_detalhes') }}
    )

    , joined as (
        select
            ordem_detalhes.id_pedido 
            , ordem_detalhes.id_produto
            , ordens.id_funcionario
            , ordens.id_cliente
            , ordens.id_transportadora
            , ordem_detalhes.desconto
            , ordem_detalhes.preco_da_unidade
            , ordem_detalhes.quantidade
            , ordens.data_do_pedido
            , ordens.frete
            , ordens.destinatario
            , ordens.endereco_destinatario
            , ordens.cep_destinatario
            , ordens.cidade_destinatario
            , ordens.regiao_destinatario
            , ordens.pais_destinatario
            , ordens.data_do_envio
            , ordens.data_requerida
        from ordem_detalhes
        inner join ordens on
            ordem_detalhes.id_pedido = ordens.id_pedido
    )

select *
from joined