SELECT
  `index` AS id,
  `Carimbo de data/hora` AS updated_at,
  `Qual tipo de massa você gosta?` AS tipo_massa,
  `Gostaria de Borda recheada?` AS borda_recheada,
  `Escolha o primeiro queijo para compor sua pizza` AS queijo_1,
  `Escolha o segundo queijo para compor sua pizza` AS queijo_2,
  `Ingrediente 1` AS ingrediente_1,
  `Ingrediente 2` AS ingrediente_2,
  `Ingrediente 3` AS ingrediente_3,
  `Ingrediente 4` AS ingrediente_4,
  `Ingrediente 5` AS ingrediente_5,
  `Escolha uma Bebida` AS bebida,
  `Acompanha Ketchup?` AS ketchup,
  `Qual estado você mora?` AS estado,
  `Algum recado para os pizzaiolo Jeferson Fernando e Téo Calvo?` AS recado_pizzaiolo

  FROM {table}

  QUALIFY row_number() OVER (PARTITION BY `index` ORDER BY `Carimbo de data/hora`) = 1