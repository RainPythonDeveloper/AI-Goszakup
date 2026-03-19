-- ============================================================
-- Индексы для ускорения запросов AI-агента
-- ============================================================

-- Справочники: поиск по коду
CREATE INDEX IF NOT EXISTS idx_ref_kato_region ON ref_kato(region);
CREATE INDEX IF NOT EXISTS idx_ref_enstru_parent ON ref_enstru(parent_code);

-- Планы
CREATE INDEX IF NOT EXISTS idx_plans_bin ON plans(bin);
CREATE INDEX IF NOT EXISTS idx_plans_enstru ON plans(enstru_code);
CREATE INDEX IF NOT EXISTS idx_plans_year ON plans(year);

-- Объявления
CREATE INDEX IF NOT EXISTS idx_announcements_number ON announcements(number_anno);
CREATE INDEX IF NOT EXISTS idx_announcements_organizer ON announcements(organizer_bin);
CREATE INDEX IF NOT EXISTS idx_announcements_customer ON announcements(customer_bin);
CREATE INDEX IF NOT EXISTS idx_announcements_publish_date ON announcements(publish_date);
CREATE INDEX IF NOT EXISTS idx_announcements_status ON announcements(status_id);

-- Лоты (самая частая таблица для аналитики)
CREATE INDEX IF NOT EXISTS idx_lots_announcement ON lots(announcement_id);
CREATE INDEX IF NOT EXISTS idx_lots_enstru ON lots(enstru_code);
CREATE INDEX IF NOT EXISTS idx_lots_customer ON lots(customer_bin);
CREATE INDEX IF NOT EXISTS idx_lots_delivery_kato ON lots(delivery_kato);
CREATE INDEX IF NOT EXISTS idx_lots_status ON lots(status_id);
CREATE INDEX IF NOT EXISTS idx_lots_price ON lots(price_per_unit);
-- Полнотекстовый поиск по названиям лотов (для нечётких запросов)
CREATE INDEX IF NOT EXISTS idx_lots_name_trgm ON lots USING gin(name_ru_clean gin_trgm_ops);

-- Договоры
CREATE INDEX IF NOT EXISTS idx_contracts_announcement ON contracts(announcement_id);
CREATE INDEX IF NOT EXISTS idx_contracts_supplier ON contracts(supplier_bin);
CREATE INDEX IF NOT EXISTS idx_contracts_customer ON contracts(customer_bin);
CREATE INDEX IF NOT EXISTS idx_contracts_sign_date ON contracts(sign_date);
CREATE INDEX IF NOT EXISTS idx_contracts_status ON contracts(status_id);
CREATE INDEX IF NOT EXISTS idx_contracts_lot ON contracts(lot_id);

-- Предметы договора (ключевая таблица для Fair Price)
CREATE INDEX IF NOT EXISTS idx_contract_subjects_contract ON contract_subjects(contract_id);
CREATE INDEX IF NOT EXISTS idx_contract_subjects_enstru ON contract_subjects(enstru_code);
CREATE INDEX IF NOT EXISTS idx_contract_subjects_kato ON contract_subjects(delivery_kato);

-- Заявки
CREATE INDEX IF NOT EXISTS idx_applications_announcement ON applications(announcement_id);
CREATE INDEX IF NOT EXISTS idx_applications_supplier ON applications(supplier_bin);
CREATE INDEX IF NOT EXISTS idx_applications_lot ON applications(lot_id);

-- Платежи
CREATE INDEX IF NOT EXISTS idx_payments_contract ON payments(contract_id);
CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(payment_date);

-- Акты
CREATE INDEX IF NOT EXISTS idx_contract_acts_contract ON contract_acts(contract_id);

-- Служебные
CREATE INDEX IF NOT EXISTS idx_raw_data_endpoint ON raw_data(source_endpoint);
CREATE INDEX IF NOT EXISTS idx_raw_data_processed ON raw_data(processed) WHERE NOT processed;
CREATE INDEX IF NOT EXISTS idx_sync_journal_processed ON sync_journal(processed) WHERE NOT processed;
CREATE INDEX IF NOT EXISTS idx_sync_journal_entity ON sync_journal(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_dq_log_table ON data_quality_log(table_name);
CREATE INDEX IF NOT EXISTS idx_dq_log_severity ON data_quality_log(severity);
