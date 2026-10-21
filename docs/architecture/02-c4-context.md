# 02 — C4 Level 1: System Context

Who uses the platform and which external systems it depends on.

```mermaid
C4Context
  title System context — supplier financial risk platform (hypothetical)

  Person(buyer, "Buyer", "Procurement / finance team ordering assessments and monitoring")
  Person(supplier, "Supplier", "Company being assessed; provides financial statements")
  Person(analyst, "Analyst", "Reviews outputs, writes narratives, runs consultations")

  System(platform, "CreditSource platform", "Assessments, monitoring, custom financial data")

  System_Ext(registries, "Registries", "ASIC, ABR")
  System_Ext(bureaus, "Credit bureaus", "Equifax, illion")
  System_Ext(courts, "Court feeds", "Insolvency and court actions")
  System_Ext(clientsys, "Client systems", "ERP (Ariba, Coupa, SAP), GRC (Archer)")
  System_Ext(partners, "Data partners", "Creditsafe, CreditWorks, CreditorWatch")

  Rel(buyer, platform, "Orders reports, views dashboards")
  Rel(supplier, platform, "Uploads financials")
  Rel(analyst, platform, "Reviews and publishes")
  Rel(platform, registries, "Company status, directors")
  Rel(platform, bureaus, "Credit data")
  Rel(platform, courts, "Legal events")
  Rel(platform, clientsys, "Scores, alerts via API/webhooks")
  Rel(platform, partners, "Financial datasets")
```

## Actors

| Actor | Role |
|---|---|
| Buyer | Provides supplier name, ABN/ACN, contact and contract value; receives report, consultation, alerts |
| Supplier | Subject of the assessment; uploads statements; triggers the 48-hour clock |
| Analyst | Reviews every report and alert before release |

## External systems

| System | Direction | Purpose |
|---|---|---|
| ASIC / ABR | In | Company status, director changes, industry classification |
| Equifax / illion | In | Credit history, defaults, enquiries |
| Court / insolvency feeds | In | Winding-up applications, judgments |
| Client ERP / GRC | Out | Ratings and alerts at award time and during contract |
| Data partners | Out | Supply of AU/NZ financial data |
