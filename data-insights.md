# Olist E-Commerce — Análise de Dados e Insights (Camada Silver)
Documentação técnica do processo de exploração e geração de insights a partir da camada Silver. Esta etapa foca em transformar os dados já tratados em conhecimento estratégico sobre logística, comportamento do consumidor e performance operacional.

## 1. Visão Geral
Após o tratamento na camada Silver, os dados estão padronizados e tipados. Esta fase do projeto utiliza um ambiente de **Jupyter Notebook** para realizar análises estatísticas e visualizações gráficas que auxiliam na compreensão de gargalos logísticos e padrões de venda do ecossistema Olist.

## 2. Como rodar a análise
Para reproduzir as análises e gerar os gráficos de insights, siga os passos abaixo:

### 2.1 Configuração do Ambiente
1. **Criar um ambiente virtual (venv):**
   ```bash
   python -m venv venv
   source venv/bin/activate # Linux/Mac
   \venv\Scripts\activate # Windows
   ```
2. **Instalar os requisitos:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Registrar o kernel no Jupyter:**
   ```bash
   python -m ipykernel install --user --name=bigdata-ecommerce --display-name "Python (BigData Ecommerce)"
   ```

### 2.2 Execução
1. **Data Loader:** Certifique-se de que o arquivo 'src/data_loader.py' está configurado. Ele automatiza a leitura de todos os CSVs da pasta 'data/silver/' para um dicionário de DataFrames.
2. **Notebook:** Abra o arquivo 'notebooks/silver_notebook.ipynb'.
3. **Seleção do Kernel:** No canto superior direito do VSCode/Jupyter, selecione o kernel "Python (BigData Ecommerce)".
4. **Rodar Células:** Execute as células em sequência para carregar os dados e gerar as visualizações.

## 3. Arquitetura de Insights
A análise foca em três pilares principais:
<ul>
  <li><strong>Logística:</strong> Eficiência de entrega e precisão de prazos.</li>
  <li><strong>Temporalidade:</strong> Comportamento de compra por períodos.</li>
  <li><strong>Geografia:</strong> Performance regional e taxas de atraso.</li>
</ul>

## 4. Insights e Visualizações
### 4.1 Relação entre tempo estimado x tempo real
Analisa a saúde operacional da base de dados, identificando o volume de pedidos que concluíram o ciclo logístico versus cancelamentos e pendências.
<img width="876" height="548" alt="Image" src="https://github.com/user-attachments/assets/4abe26bc-989c-41eb-8925-751d1d08190f" />
**Principais Insights:**
<ul>
  <li>
    <strong>Predomínio de Entregas:</strong> Mais de 97% dos pedidos possuem status 'delivered', confirmando a maturidade do ecossistema e a viabilidade dos dados para modelagem de caminhos de sucesso.
  </li>
  <li>
    <strong>Baixo Atrito:</strong> A taxa de cancelamento é mínima, indicando alta confiança no fluxo transacional e baixa ocorrência de falhas críticas de sistema.
  </li>
</ul>

### 4.2 Tempo Médio de Entrega por Estado
Identifica disparidades geográficas na performance logística brasileira, correlacionando o tempo de entrega com a localização do cliente.
<img width="847" height="704" alt="Image" src="https://github.com/user-attachments/assets/ae2ef76c-4e91-4cd5-9b64-6a07c985209e" />
**Principais Insights:**
<ul>
  <li>
    <strong>Desigualdade Regional:</strong> Estados do Norte e Nordeste apresentam tempos de entrega até 3x superiores aos de SP, refletindo desafios de infraestrutura continental.
  </li>
  <li>
    <strong>Eficiência do Hub Sudeste:</strong> O baixo tempo em SP evidencia a vantagem estratégica de estar próximo aos centros de distribuição e à maioria dos vendedores.
  </li>
</ul>

### 4.3 Distribuição da Gravidade dos Atrasos
Foca na experiência negativa do cliente, medindo quantos dias a entrega excedeu o prazo prometido originalmente.
<img width="1023" height="548" alt="Image" src="https://github.com/user-attachments/assets/f1f6066b-9d48-4f3c-bbec-bc74dc742c5f" />
**Principais Insights:**
<ul>
  <li>
    <strong>Recuperação Logística:</strong> A maioria dos atrasos concentra-se entre 1 e 5 dias, demonstrando que a malha operacional consegue corrigir desvios de rota rapidamente.
  </li>
  <li>
    <strong>Outliers Críticos:</strong> A "cauda longa" do gráfico mostra casos raros que passam de 30 dias, representando falhas graves que impactam diretamente a satisfação no grafo de avaliações.
  </li>
</ul>

### 4.4 Relação entre tempo estimado x tempo real
Analisa a precisão do algoritmo de frete da Olist comparando o que foi prometido ao cliente versus o que foi realizado.
<img width="850" height="547" alt="image" src="https://github.com/user-attachments/assets/ad873409-b03d-47cf-b05a-f5be4e267bad" />
**Principais Insights:**
<ul>
  <li>
    <strong>Conservadorismo:</strong> A maioria dos pontos está abaixo da linha vermelha, indicando que a Olist entrega antes do prazo prometido.
  </li>
  <li>
    <strong>Outliers de atraso:</strong> Pontos acima da linha representam falahs críticas na cadeia logística (extrativos ou problemas graves).
  </li>
</ul>

### 4.5 Volume de comprar por dia da semana
Identifica o "ritmo cardíaco" das vendas para otimização de campanhas de marketing e escala operacional.
<img width="868" height="548" alt="image" src="https://github.com/user-attachments/assets/b3eedc22-669c-472c-8b4b-56a7a0db6c57" />
**Principais Insights:**
<ul>
  <li>
    <strong>Pico semanal:</strong> Segunda e Terça-feira concentram o maior volume de pedidos, refletindo decisões de compra tomadas durante o final de semana.
  </li>
  <li>
    <strong>Dia com menor volume de vendas:</strong> O Sábado é o dia de menor engajamento, sugerindo a necessidade de promoções específicas para este período.
  </li>
</ul>

### 4.6 Percentual de pedidos com atraso por mês
Monitora a saúde da operação ao longo do tempo, identificando crises e sazonalidades.
<img width="1189" height="590" alt="image" src="https://github.com/user-attachments/assets/3f9face4-bc4c-4254-b053-0326b544f08c" />
**Principais Insights:**
<ul>
  <li>
    <strong>Gargalo de sazonalidade:</strong> Picos claros em Novembro e Dezembro (Black Friday e Natal).
  </li>
  <li>
    <strong>Anomalias históricas:</strong> O pico de Março/2018 (18.4%) evidencia o impacto de eventos externos (como greves logísticas) na satisfação do cliente.
  </li>
</ul>

## 5. Próximos passos
Os dados limpos e os insights gerados aqui servem de base para a Camada Gold, onde os relacionamentos serão mapeados no banco de dados em grafos Neo4j, permitindo análises complexas de rede entre Clientes, Vendedores e Produtos.
