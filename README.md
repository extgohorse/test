# test
RESPOSTAS TEORICAS

1- Persistir na memoria um rdd/dataframe para que o acesso aos dados seja feito de forma mais performaatica

2- Pois a engine do spark trabalha in-memory, o que torna o acesso as dados mais veloz, ja o mapreduce tem mais i/o em disco

3- SparkContext e o point-entry para as funcionalidades do spark, como criacao e manipulacao de rdds

4- RDD e uma das abstracoes para manipulacao e processamento de dados do spark.

5- Pois o reduce by key pre-combina as chaves em comum antes de realizar o merge do resultados de cada executores, evitando assim maior trafego de rede

6- Quantidade de ocorrencias por palavra

RESPOSTAS PRATICAS

As respostas praticas foram desenvolvidas utilizando a linguagem python.

-Os dados da Nasa foram enviados para o hdfs, na pasta /user/root/nasa
-Para rodar o programa: spark-submit testesemantix.py
