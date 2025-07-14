create table dbo.DICT_COR
(
    color_name varchar(20) not null
        constraint dicionario_cor_pk
            primary key,
    cor        varchar(20)
)
go

INSERT INTO LUX_OPENDATA.dbo.DICT_COR (color_name, cor) VALUES (N'beige', N'Bege');
INSERT INTO LUX_OPENDATA.dbo.DICT_COR (color_name, cor) VALUES (N'blanc', N'Branco');
INSERT INTO LUX_OPENDATA.dbo.DICT_COR (color_name, cor) VALUES (N'bleu', N'Azul');
INSERT INTO LUX_OPENDATA.dbo.DICT_COR (color_name, cor) VALUES (N'brun', N'Castanho');
INSERT INTO LUX_OPENDATA.dbo.DICT_COR (color_name, cor) VALUES (N'gris', N'Cinzento');
INSERT INTO LUX_OPENDATA.dbo.DICT_COR (color_name, cor) VALUES (N'jaune', N'Amarelo');
INSERT INTO LUX_OPENDATA.dbo.DICT_COR (color_name, cor) VALUES (N'noir', N'Preto');
INSERT INTO LUX_OPENDATA.dbo.DICT_COR (color_name, cor) VALUES (N'orange', N'Laranja');
INSERT INTO LUX_OPENDATA.dbo.DICT_COR (color_name, cor) VALUES (N'rouge', N'Encarnado');
INSERT INTO LUX_OPENDATA.dbo.DICT_COR (color_name, cor) VALUES (N'vert', N'Verde');
INSERT INTO LUX_OPENDATA.dbo.DICT_COR (color_name, cor) VALUES (N'violet', N'Roxo');
