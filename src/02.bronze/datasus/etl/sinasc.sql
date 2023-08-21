SELECT *,
        coalesce(contador, 'NA') ||
        coalesce(CODESTAB, 'NA') ||
        coalesce(CODMUNNASC, 'NA') ||
        coalesce(NUMEROLOTE, 'NA') ||
        coalesce(DTCADASTRO, 'NA') ||
        coalesce(DTNASC, 'NA') ||
        coalesce(substring(HORANASC,0,4), 'NA') ||
        coalesce(DTNASCMAE, 'NA') AS IDSINASC,
        
        to_timestamp( coalesce(DTNASC, '00000000') ||
            coalesce(
                substring(
                    rpad(
                        replace(replace(HORANASC,'-',''),4,'0'),
                        'h','-')
                    ,0,4),
                '0000'),
            'ddMMyyyyHHmm') AS DATAHORANASC

FROM {table}

QUALIFY row_number() OVER (PARTITION BY IDSINASC ORDER BY DATAHORANASC DESC) = 1