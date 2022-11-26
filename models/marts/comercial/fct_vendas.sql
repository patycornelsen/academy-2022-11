with
    clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )

    , funcionarios as (
        select *
        from {{ ref('dim_funcionarios') }}
    )

    , transportadoras as (
        select *
        from {{ ref('dim_transportadoras') }}
    )

    , produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , pedidos_item as (
        select *
        from {{ ref('int_vendas__pedido_itens') }}
    )

    , joined as (
        select
            pedidos_item.id_pedido 
            , clientes.sk_cliente as fk_cliente
            , funcionarios.sk_funcionario as fk_funcionario
            , transportadoras.sk_transportadora as fk_transportadora
            , produtos.sk_produto as fk_produto
            , pedidos_item.desconto
            , pedidos_item.preco_da_unidade
            , pedidos_item.quantidade
            , pedidos_item.data_do_pedido
            , pedidos_item.frete
            , pedidos_item.destinatario
            , pedidos_item.endereco_destinatario
            , pedidos_item.cep_destinatario
            , pedidos_item.cidade_destinatario
            , pedidos_item.regiao_destinatario
            , pedidos_item.pais_destinatario
            , pedidos_item.data_do_envio
            , pedidos_item.data_requerida
            , clientes.nome_do_cliente
            , funcionarios.nome_completo_funcionario
            , transportadoras.nome_transportadora
            , produtos.nome_produto
            , produtos.nome_categoria
            , produtos.nome_do_fornecedor
            , produtos.is_discontinuado
        from pedidos_item
        left join clientes on
            pedidos_item.id_cliente = clientes.id_cliente
        left join funcionarios on
            pedidos_item.id_funcionario = funcionarios.id_funcionario
        left join transportadoras on
            pedidos_item.id_transportadora = transportadoras.id_transportadora
        left join produtos on
            pedidos_item.id_produto = produtos.id_produto
    )

    , transformed as (
        select
            {{ dbt_utils.surrogate_key (['id_pedido', 'fk_produto']) }} as sk_vendas
            , *
            , preco_da_unidade * quantidade as total_bruto
            , (1 - desconto) * preco_da_unidade * quantidade as total_liquido
        from joined
    )

select *
from transformed