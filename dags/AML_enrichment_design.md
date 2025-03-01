**High-Level Design Document: Enrichment Layer for AML & Fraud Detection Pipeline**

---

**Overview**  
The enrichment layer processes data from multiple upstream feeds stored in Amazon S3 and transforms it into a consolidated “index” dataset. This index—produced through complex left-joins, data enrichment, and ML score integration—is then used as the definitive batch output for downstream systems (e.g., compliance dashboards, case management). The system is designed to meet production-level complexity and scale while providing a robust, auditable data model.

**Functional Requirements**  
- Read the latest partitions from all upstream feeds (customer profiles, account relationships, transactions, merchant data, watchlist, device channel, KYC questionnaires, and complaints).  
- Perform comprehensive left joins to combine these feeds into a unified view.  
- Enrich the joined data with internally generated policy data (AML compliance rules) and ML outputs (suspicion scores, fraud signals).  
- Produce an “index” document (suspicious transaction/entity index) that aggregates risk scores and flags for further investigation.  
- Serve this index in a batch mode for downstream consumption by compliance and legal teams.

**Non-Functional Requirements**  
- **Scalability:** The system must efficiently handle millions of transactions and updates daily. Partitioning and parallel processing ensure horizontal scaling.  
- **Performance & Latency:** Batch processes must complete within defined SLAs (e.g., nightly reprocessing within a few hours) while future enhancements (streaming integration) target sub-minute latency improvements.  
- **Reliability:** Data integrity is ensured via robust error handling, checkpointing in Spark, and comprehensive logging.  
- **Maintainability:** The design leverages modular Spark jobs, centralized metadata via AWS Glue Catalog, and infrastructure as code (Terraform) to simplify updates and scaling.

**Architecture & Data Flow**  
- **Data Sources:**  
  - Upstream feeds (8 data sources) are partitioned by date, region, or environment in S3.  
  - Internal policy data (AML compliance rules) is maintained separately and refreshed periodically.

- **Processing Framework:**  
  - **Batch Processing on AWS EMR:** The primary approach is batch processing using Spark on EMR. This layer reads the latest S3 partitions from each upstream dataset.  
  - **Enrichment Workflow:**  
    - Perform left joins between the primary transaction feed and supporting datasets (customer profiles, account relationships, merchant data, etc.).  
    - Apply enrichment logic including ML scoring (suspicion scores), rule-based flags (e.g., watchlist matches), and compliance thresholds from internal policy data.  
    - Derive dimension and fact tables following a star schema (with optional snowflake extensions for hierarchical dimensions such as location details).

- **Design Model:**  
  - The system follows a **Lambda Architecture** for now:  
    - **Batch Layer:** Performs full joins and re-computations nightly to produce a consolidated index.  
    - **Speed Layer (Future Enhancement):** Plans to incorporate Spark Structured Streaming to reduce latency on critical datasets.  
  - The data model uses a **Star Schema** for efficient querying and analytics, with fact tables (e.g., Fact_Transactions) and dimension tables (Dim_Customer, Dim_Merchant, etc.). For more detailed hierarchies (e.g., location and watchlist details), a **Snowflake Design** will be employed.

- **Output:**  
  - The final enriched index is written to S3 as partitioned Parquet files.  
  - This index serves as the input for downstream systems such as compliance dashboards and case management tools.

**Data Model and Schema Design**  
- **Fact Tables:**  
  - Fact_Transactions: Contains transactional measures (amount, suspicion_score, etc.) and foreign keys to dimensions.  
- **Dimension Tables:**  
  - Dim_Customer, Dim_Merchant, Dim_Date, Dim_Account, etc.  
  - Additional dimensions for device information or watchlist details may be maintained in a snowflake structure.
- **Indexes:**  
  - Suspicious Transaction Index and Suspicious Entity Index aggregate risk information for rapid query and investigation.

**Deployment and Orchestration**  
- **Infrastructure Provisioning:**  
  - All AWS resources (S3 buckets, EMR clusters, MSK topics, Glue Catalog, IAM roles) are provisioned using Terraform for consistency across dev, test, and production environments.
- **Orchestration:**  
  - AWS Step Functions or Apache Airflow (deployed on EC2/EKS) orchestrates the enrichment pipeline.  
  - Batch jobs are scheduled nightly or every few hours, while real-time streaming components (when integrated) will run continuously.
- **Data Quality & Performance Testing:**  
  - Automated data quality checks (e.g., schema validation, null rate verification) are integrated as tasks in the orchestration DAG.
  - Performance tests simulate high transaction volumes to ensure processing latency and throughput meet defined SLAs.
- **Monitoring & Alerting:**  
  - EMR and Spark metrics are monitored via CloudWatch.
  - Alarms and notifications are set for job failures, increased latency, or abnormal data volumes.

**Future Enhancements**  
- **Stream Processing:**  
  - Integrate Spark Structured Streaming to reduce batch processing latency, effectively moving toward a hybrid Lambda/Kappa architecture.  
- **Dynamic Partitioning:**  
  - Implement dynamic partition discovery and compaction to manage small files and improve query performance.  
- **Auto-Scaling Improvements:**  
  - Enhance auto-scaling policies on EMR to dynamically adjust resources based on real-time workload.

---

**Entity Relationship Model (Schemas Overview)**

```
[ customer_profile ]
  - customer_id (PK)
  - full_name
  - date_of_birth
  - nationality
  - primary_address { street, city, postal_code, country }
  - contact_info { email, phone }
  - kyc_status
  - risk_score
  - created_at
  - updated_at

        └───< One-to-Many >───┐
                               │
                         [ account_rel ]
                           - account_id (PK)
                           - customer_id (FK)
                           - account_type
                           - open_date
                           - status
                           - balance
                           - currency_code
                           - branch_id
                           - last_updated

                               │
                               └───< One-to-Many >───┐
                                                       │
                                                 [ transactions ]
                                                   - transaction_id (PK)
                                                   - account_id (FK)
                                                   - customer_id (FK)
                                                   - merchant_id (FK)
                                                   - txn_timestamp
                                                   - amount
                                                   - currency_code
                                                   - channel
                                                   - geo_location { lat, lon }
                                                   - device_info (Map)
                                                   - status

[ merchant_data ]
  - merchant_id (PK)
  - legal_name
  - categories (Array)
  - risk_level
  - contact { phone, email }
  - address (Map)
  - created_at
  - updated_at

[ watchlist ]
  - entity_name
  - entity_type
  - risk_category
  - listing_source
  - watchlist_date
  - notes
  - last_updated

[ device_channel ]
  - device_id
  - customer_id (FK)
  - device_type
  - ip_address
  - os_version
  - login_time
  - location { lat, lon }
  - suspicious_flags (Array)

[ kyc_questionnaire ]
  - form_id (PK)
  - customer_id (FK)
  - question_responses (Map)
  - review_status
  - last_reviewed_by
  - review_date
  - updated_at

[ complaints ]
  - complaint_id (PK)
  - customer_id
  - account_id (optional)
  - txn_id (optional)
  - opened_date
  - complaint_type
  - status
  - resolution_notes (Array)
  - last_updated

[ aml_compliance_rules ]
  - rule_id (PK)
  - description
  - threshold_params (Map)
  - effective_date
  - version
  - owner
  - updated_at

----------------------------
Downstream Indexes:
----------------------------
[ suspicious_tx_index ]
  - transaction_id (FK)
  - customer_id (FK)
  - suspicion_score
  - rule_hits (Array)
  - flagged_time
  - status
  - assigned_officer

[ suspicious_entity_index ]
  - entity_id
  - entity_type
  - aggregated_score
  - reasons (Array)
  - last_updated

[ aml_case ]
  - case_id (PK)
  - subject_id (FK)
  - subject_type
  - opened_date
  - details (Map)
  - assigned_group
  - status
  - resolution
  - last_updated
```

---