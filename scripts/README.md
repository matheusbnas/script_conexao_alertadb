# 📁 Scripts Principais

Scripts organizados por ambiente de execução.

---

## 📂 Estrutura

```
scripts/
├── servidor166/          # Scripts para máquina virtual (servidor 166)
│   ├── carregar_pluviometricos_historicos.py  # Carga inicial completa
│   ├── sincronizar_pluviometricos_novos.py   # Sincronização incremental
│   ├── validar_dados_pluviometricos.py       # Validação de dados
│   ├── exportar_pluviometricos_parquet.py    # Exportação para Parquet
│   ├── app.py                                 # API REST Flask
│   └── dashboard.html                         # Dashboard web
│
└── cloudsql/            # Scripts para Cloud SQL GCP
    ├── carregar_para_cloudsql_inicial.py     # Carga inicial Cloud SQL
    └── sincronizar_para_cloudsql.py          # Sincronização incremental Cloud SQL
```

---

## 🖥️ Servidor 166

**Carga inicial (executar uma vez):**
```bash
python scripts/servidor166/carregar_pluviometricos_historicos.py
```

**Sincronização incremental:**
```bash
# Modo contínuo
python scripts/servidor166/sincronizar_pluviometricos_novos.py

# Modo único (para cron)
python scripts/servidor166/sincronizar_pluviometricos_novos.py --once
```

**API REST:**
```bash
python scripts/servidor166/app.py
```

**Utilitários:**
- `validar_dados_pluviometricos.py` - Valida integridade dos dados
- `exportar_pluviometricos_parquet.py` - Exporta para formato Parquet

---

## ☁️ Cloud SQL

**Carga inicial (executar uma vez):**
```bash
python scripts/cloudsql/carregar_para_cloudsql_inicial.py
```

**Sincronização incremental:**
```bash
# Modo contínuo
python scripts/cloudsql/sincronizar_para_cloudsql.py

# Modo único (para cron)
python scripts/cloudsql/sincronizar_para_cloudsql.py --once
```

---

## 📚 Documentação

- [README Principal](../README.md)
- [Automação](../automacao/README.md)
- [Documentação Completa](../docs/)
