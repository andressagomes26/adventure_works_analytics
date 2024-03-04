# <h1 align="center"><font color = #0081be>Adventure Works | Desafio Final de Dados 🚴‍♀️</font></h1>
Neste projeto, são realizadas transformações e análise dos dados brutos da indústria de bicicletas Adventure Works (AW), utilizado a ferramenta dbt.

<div align="center"><img src='https://d1muf25xaso8hp.cloudfront.net/https%3A%2F%2Ff2fa1cdd9340fae53fcb49f577292458.cdn.bubble.io%2Ff1704517258916x341896913921438500%2FAdventureWorks_Logo.png?w=&h=&auto=compress&dpr=1&fit=max' style='width: 50%;'></div>

## Instalar e executar o projeto

Para utilizar este projeto deve-se clonar o repositório do github e seguir as seguintes etapas:

- Criar um arquivo de credenciais, como: ```/home/user/.dbt/profiles.yml```

- Criar ambiente virtual:

```bash
python -m venv venv
```

- Ativar ambiente virtual:
```
source venv/bin/activate
```

- Instalar dependências: 
```bash
pip install -r requirements.txt
```

- Entrar na pasta do projeto:
```bash
cd projectaw/
```

- Executar os modelos:
```bash
dbt run
```

- Executar os testes:
```bash
dbt test
```
