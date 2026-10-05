CREATE TABLE IF NOT EXISTS `ulb_user_profile` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `town_id` INT NOT NULL,
  `district_id` INT DEFAULT NULL,
  `ulb_name` VARCHAR(255) DEFAULT NULL,
  `district_name` VARCHAR(255) DEFAULT NULL,
  `ulb_type` VARCHAR(100) DEFAULT NULL,
  `mc_eo_details` TEXT DEFAULT NULL,
  `nodal_officer_details` TEXT DEFAULT NULL,
  `num_wards` INT DEFAULT NULL,
  `geo_area_sqkm` DECIMAL(10,2) DEFAULT NULL,
  `population_households` TEXT DEFAULT NULL,
  `ulb_known_for` TEXT DEFAULT NULL,
  `top_complaint_services` TEXT DEFAULT NULL,
  
  /* Assets & Land */
  `total_properties` INT DEFAULT NULL,
  `total_land_parcels` INT DEFAULT NULL,
  `total_land_area_acres` DECIMAL(10,2) DEFAULT NULL,
  `vacant_land_parcels` INT DEFAULT NULL,
  `encroached_land_acres` DECIMAL(10,2) DEFAULT NULL,
  `road_length_km` DECIMAL(10,2) DEFAULT NULL,
  `asset_register_status` VARCHAR(50) DEFAULT NULL,
  `asset_update_freq` VARCHAR(50) DEFAULT NULL,
  `total_ulb_shops` INT DEFAULT NULL,
  `vacant_ulb_shops` INT DEFAULT NULL,
  `shops_annual_income_lakh` DECIMAL(12,2) DEFAULT NULL,
  `municipal_markets_count` INT DEFAULT NULL,
  `markets_annual_income_lakh` DECIMAL(12,2) DEFAULT NULL,
  `vacant_building_available` VARCHAR(10) DEFAULT NULL,
  `land_for_comm_dev` VARCHAR(10) DEFAULT NULL,
  `property_for_redev_ppp` VARCHAR(10) DEFAULT NULL,
  
  /* Community Infrastructure */
  `community_halls_count` INT DEFAULT NULL,
  `sports_grounds_count` INT DEFAULT NULL,
  `libraries_count` INT DEFAULT NULL,
  
  /* Digital Infrastructure */
  `cctv_total_installed` INT DEFAULT NULL,
  `cctv_functional` INT DEFAULT NULL,
  `central_control_room` VARCHAR(10) DEFAULT NULL,
  
  /* Energy */
  `total_streetlights` INT DEFAULT NULL,
  `non_functional_streetlights` INT DEFAULT NULL,
  `streetlights_elec_exp_lakh` DECIMAL(12,2) DEFAULT NULL,
  
  /* Finance & Accounts */
  `accounting_system` VARCHAR(100) DEFAULT NULL,
  `accounts_prepared_fy` VARCHAR(50) DEFAULT NULL,
  `accounts_audited_fy` VARCHAR(50) DEFAULT NULL,
  `closing_cash_balance_lakh` DECIMAL(12,2) DEFAULT NULL,
  `total_income_lakh` DECIMAL(12,2) DEFAULT NULL,
  `total_expenditure_lakh` DECIMAL(12,2) DEFAULT NULL,
  `own_source_income_lakh` DECIMAL(12,2) DEFAULT NULL,
  `state_grants_lakh` DECIMAL(12,2) DEFAULT NULL,
  `central_grants_lakh` DECIMAL(12,2) DEFAULT NULL,
  `has_outstanding_loan` VARCHAR(10) DEFAULT NULL,
  `outstanding_loan_lakh` DECIMAL(12,2) DEFAULT NULL,
  `has_credit_rating` VARCHAR(10) DEFAULT NULL,
  `user_charges_collected_services` TEXT DEFAULT NULL,
  `revenue_breakdown_json` LONGTEXT DEFAULT NULL,
  `property_tax_registered_count` INT DEFAULT NULL,
  `property_tax_paid_count` INT DEFAULT NULL,
  `property_tax_demand_lakh` DECIMAL(12,2) DEFAULT NULL,
  `property_tax_collected_lakh` DECIMAL(12,2) DEFAULT NULL,
  `property_tax_arrears_lakh` DECIMAL(12,2) DEFAULT NULL,
  `property_tax_gis_linked` VARCHAR(50) DEFAULT NULL,
  
  /* Health & Education */
  `stray_cattle_count` INT DEFAULT NULL,
  `abc_programme_dogs` VARCHAR(10) DEFAULT NULL,
  `stray_dogs_sterilised_annual` INT DEFAULT NULL,
  `stray_animal_exp_lakh` DECIMAL(12,2) DEFAULT NULL,
  `health_dispensaries_count` INT DEFAULT NULL,
  `govt_schools_count` INT DEFAULT NULL,
  `anganwadi_centres_count` INT DEFAULT NULL,
  `anganwadi_in_ulb_building` INT DEFAULT NULL,
  
  /* Horticulture */
  `parks_count` INT DEFAULT NULL,
  `tree_register_status` VARCHAR(50) DEFAULT NULL,
  `tree_count_recorded` INT DEFAULT NULL,
  `parks_exp_lakh` DECIMAL(12,2) DEFAULT NULL,
  `nurseries_count` INT DEFAULT NULL,
  
  /* Institutional */
  `revised_taxes_last3yr` VARCHAR(10) DEFAULT NULL,
  `priority_investment_areas` TEXT DEFAULT NULL,
  `ppp_project_experience` VARCHAR(100) DEFAULT NULL,
  `identified_ppp_projects_detail` TEXT DEFAULT NULL,
  `sanctioned_posts` INT DEFAULT NULL,
  `permanent_employees` INT DEFAULT NULL,
  `contractual_employees` INT DEFAULT NULL,
  `vacant_sanctioned_posts` INT DEFAULT NULL,
  
  /* Mobility */
  `footpaths_length_km` DECIMAL(10,2) DEFAULT NULL,
  `auth_parking_locations` INT DEFAULT NULL,
  `parking_vehicle_capacity` INT DEFAULT NULL,
  `parking_annual_income_lakh` DECIMAL(12,2) DEFAULT NULL,
  `need_additional_parking` VARCHAR(10) DEFAULT NULL,
  `public_bus_service` VARCHAR(10) DEFAULT NULL,
  `buses_operating_count` INT DEFAULT NULL,
  `bus_stands_count` INT DEFAULT NULL,
  `ulb_owned_bus_stands` INT DEFAULT NULL,
  `bus_stand_annual_income_lakh` DECIMAL(12,2) DEFAULT NULL,
  `land_near_bus_stand_comm` VARCHAR(10) DEFAULT NULL,
  `ev_charging_stations_count` INT DEFAULT NULL,
  `land_for_ev_charging` VARCHAR(10) DEFAULT NULL,
  
  /* Public Amenities */
  `functional_public_toilets` INT DEFAULT NULL,
  `cremation_burial_grounds` INT DEFAULT NULL,
  
  /* Urban Livelihood */
  `registered_street_vendors` INT DEFAULT NULL,
  `vending_zones_details` TEXT DEFAULT NULL,
  
  /* WASH */
  `piped_water_coverage_pct` DECIMAL(5,2) DEFAULT NULL,
  `water_connections_count` INT DEFAULT NULL,
  `metered_water_connections` INT DEFAULT NULL,
  `avg_water_supply_hours` DECIMAL(4,2) DEFAULT NULL,
  `non_revenue_water_pct` DECIMAL(5,2) DEFAULT NULL,
  `water_supply_exp_lakh` DECIMAL(12,2) DEFAULT NULL,
  `water_pumping_elec_exp_lakh` DECIMAL(12,2) DEFAULT NULL,
  `sewerage_coverage_pct` DECIMAL(5,2) DEFAULT NULL,
  `sewage_generated_mld` DECIMAL(10,2) DEFAULT NULL,
  `stp_installed_cap_mld` DECIMAL(10,2) DEFAULT NULL,
  `actual_sewage_treated_mld` DECIMAL(10,2) DEFAULT NULL,
  `treated_wastewater_reused` VARCHAR(10) DEFAULT NULL,
  `msw_generated_tpd` DECIMAL(10,2) DEFAULT NULL,
  `door_to_door_waste_cov_pct` DECIMAL(5,2) DEFAULT NULL,
  `waste_segregation_pct` DECIMAL(5,2) DEFAULT NULL,
  `swm_annual_exp_lakh` DECIMAL(12,2) DEFAULT NULL,
  `legacy_waste_dumpsite` VARCHAR(10) DEFAULT NULL,
  
  `updated_by` VARCHAR(150) DEFAULT NULL,
  `status` VARCHAR(20) DEFAULT 'Submitted',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
