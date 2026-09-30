<?php
session_start();
error_reporting(0);
include('includes/config.php');

if (strlen($_SESSION['aid']) == 0) {
  header('location:index.php');
  exit();
}

// Auto-create table if not exists
$table_sql = "CREATE TABLE IF NOT EXISTS `ulb_user_profile` (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
$con->query($table_sql);

// Drop unique constraint on town_id if it exists to allow multiple submission entries
@$con->query("ALTER TABLE `ulb_user_profile` DROP INDEX `town_id`");

// Fetch logged in user's town and district details
$sql_login = "SELECT districtid, townid, staff_name FROM streetlightlogin WHERE id=" . (int)$_SESSION['aid'];
$res_login = $con->query($sql_login);
$user_town_id = 0;
$user_dist_id = 0;
$staff_name = '';

if ($res_login && $res_login->num_rows > 0) {
  $row_l = $res_login->fetch_assoc();
  $user_town_id = $row_l['townid'];
  $user_dist_id = $row_l['districtid'];
  $staff_name = $row_l['staff_name'];
}

// Fetch Town & District names from towns_streetlight_mapping
$ulb_name = "State Admin View";
$district_name = "All Districts";
$ulb_type = "Municipal Corporation";

if ($user_town_id > 0) {
  $sql_map = "SELECT town_name, district_name, distid FROM towns_streetlight_mapping WHERE town_id = " . (int)$user_town_id;
  $res_map = $con->query($sql_map);
  if ($res_map && $res_map->num_rows > 0) {
    $row_m = $res_map->fetch_assoc();
    $ulb_name = $row_m['town_name'];
    $district_name = $row_m['district_name'];
    $user_dist_id = $row_m['distid'];
  }
}

// Handle Form Submission
$msg = "";
$error = "";
$form_submitted_success = false;

if (isset($_POST['save_profile'])) {
  $target_town_id = ($user_town_id > 0) ? $user_town_id : (int)$_POST['selected_town_id'];
  
  if ($target_town_id <= 0) {
    $error = "Please select a valid ULB to submit profile information.";
  } else {
    // Collect form fields
    $ulb_type_val = mysqli_real_escape_string($con, $_POST['ulb_type']);
    $mc_eo_details = mysqli_real_escape_string($con, $_POST['mc_eo_details']);
    $nodal_officer_details = mysqli_real_escape_string($con, $_POST['nodal_officer_details']);
    $num_wards = !empty($_POST['num_wards']) ? (int)$_POST['num_wards'] : "NULL";
    $geo_area_sqkm = !empty($_POST['geo_area_sqkm']) ? (float)$_POST['geo_area_sqkm'] : "NULL";
    $population_households = mysqli_real_escape_string($con, $_POST['population_households']);
    
    $ulb_known_for = isset($_POST['ulb_known_for']) ? mysqli_real_escape_string($con, implode(", ", $_POST['ulb_known_for'])) : "";
    $top_complaint_services = isset($_POST['top_complaint_services']) ? mysqli_real_escape_string($con, implode(", ", $_POST['top_complaint_services'])) : "";

    /* Assets & Land */
    $total_properties = !empty($_POST['total_properties']) ? (int)$_POST['total_properties'] : "NULL";
    $total_land_parcels = !empty($_POST['total_land_parcels']) ? (int)$_POST['total_land_parcels'] : "NULL";
    $total_land_area_acres = !empty($_POST['total_land_area_acres']) ? (float)$_POST['total_land_area_acres'] : "NULL";
    $vacant_land_parcels = !empty($_POST['vacant_land_parcels']) ? (int)$_POST['vacant_land_parcels'] : "NULL";
    $encroached_land_acres = !empty($_POST['encroached_land_acres']) ? (float)$_POST['encroached_land_acres'] : "NULL";
    $road_length_km = !empty($_POST['road_length_km']) ? (float)$_POST['road_length_km'] : "NULL";
    $asset_register_status = mysqli_real_escape_string($con, $_POST['asset_register_status']);
    $asset_update_freq = mysqli_real_escape_string($con, $_POST['asset_update_freq']);
    $total_ulb_shops = !empty($_POST['total_ulb_shops']) ? (int)$_POST['total_ulb_shops'] : "NULL";
    $vacant_ulb_shops = !empty($_POST['vacant_ulb_shops']) ? (int)$_POST['vacant_ulb_shops'] : "NULL";
    $shops_annual_income_lakh = !empty($_POST['shops_annual_income_lakh']) ? (float)$_POST['shops_annual_income_lakh'] : "NULL";
    $municipal_markets_count = !empty($_POST['municipal_markets_count']) ? (int)$_POST['municipal_markets_count'] : "NULL";
    $markets_annual_income_lakh = !empty($_POST['markets_annual_income_lakh']) ? (float)$_POST['markets_annual_income_lakh'] : "NULL";
    $vacant_building_available = mysqli_real_escape_string($con, $_POST['vacant_building_available']);
    $land_for_comm_dev = mysqli_real_escape_string($con, $_POST['land_for_comm_dev']);
    $property_for_redev_ppp = mysqli_real_escape_string($con, $_POST['property_for_redev_ppp']);

    /* Community Infra */
    $community_halls_count = !empty($_POST['community_halls_count']) ? (int)$_POST['community_halls_count'] : "NULL";
    $sports_grounds_count = !empty($_POST['sports_grounds_count']) ? (int)$_POST['sports_grounds_count'] : "NULL";
    $libraries_count = !empty($_POST['libraries_count']) ? (int)$_POST['libraries_count'] : "NULL";

    /* Digital Infra */
    $cctv_total_installed = !empty($_POST['cctv_total_installed']) ? (int)$_POST['cctv_total_installed'] : "NULL";
    $cctv_functional = !empty($_POST['cctv_functional']) ? (int)$_POST['cctv_functional'] : "NULL";
    $central_control_room = mysqli_real_escape_string($con, $_POST['central_control_room']);

    /* Energy */
    $total_streetlights = !empty($_POST['total_streetlights']) ? (int)$_POST['total_streetlights'] : "NULL";
    $non_functional_streetlights = !empty($_POST['non_functional_streetlights']) ? (int)$_POST['non_functional_streetlights'] : "NULL";
    $streetlights_elec_exp_lakh = !empty($_POST['streetlights_elec_exp_lakh']) ? (float)$_POST['streetlights_elec_exp_lakh'] : "NULL";

    /* Finance & Accounts */
    $accounting_system = mysqli_real_escape_string($con, $_POST['accounting_system']);
    $accounts_prepared_fy = mysqli_real_escape_string($con, $_POST['accounts_prepared_fy']);
    $accounts_audited_fy = mysqli_real_escape_string($con, $_POST['accounts_audited_fy']);
    $closing_cash_balance_lakh = !empty($_POST['closing_cash_balance_lakh']) ? (float)$_POST['closing_cash_balance_lakh'] : "NULL";
    $total_income_lakh = !empty($_POST['total_income_lakh']) ? (float)$_POST['total_income_lakh'] : "NULL";
    $total_expenditure_lakh = !empty($_POST['total_expenditure_lakh']) ? (float)$_POST['total_expenditure_lakh'] : "NULL";
    $own_source_income_lakh = !empty($_POST['own_source_income_lakh']) ? (float)$_POST['own_source_income_lakh'] : "NULL";
    $state_grants_lakh = !empty($_POST['state_grants_lakh']) ? (float)$_POST['state_grants_lakh'] : "NULL";
    $central_grants_lakh = !empty($_POST['central_grants_lakh']) ? (float)$_POST['central_grants_lakh'] : "NULL";
    $has_outstanding_loan = mysqli_real_escape_string($con, $_POST['has_outstanding_loan']);
    $outstanding_loan_lakh = !empty($_POST['outstanding_loan_lakh']) ? (float)$_POST['outstanding_loan_lakh'] : "NULL";
    $has_credit_rating = mysqli_real_escape_string($con, $_POST['has_credit_rating']);
    $user_charges_collected_services = isset($_POST['user_charges_collected_services']) ? mysqli_real_escape_string($con, implode(", ", $_POST['user_charges_collected_services'])) : "";
    
    // Revenue JSON breakdown
    $rev_array = isset($_POST['rev']) ? $_POST['rev'] : array();
    $revenue_breakdown_json = mysqli_real_escape_string($con, json_encode($rev_array));

    $property_tax_registered_count = !empty($_POST['property_tax_registered_count']) ? (int)$_POST['property_tax_registered_count'] : "NULL";
    $property_tax_paid_count = !empty($_POST['property_tax_paid_count']) ? (int)$_POST['property_tax_paid_count'] : "NULL";
    $property_tax_demand_lakh = !empty($_POST['property_tax_demand_lakh']) ? (float)$_POST['property_tax_demand_lakh'] : "NULL";
    $property_tax_collected_lakh = !empty($_POST['property_tax_collected_lakh']) ? (float)$_POST['property_tax_collected_lakh'] : "NULL";
    $property_tax_arrears_lakh = !empty($_POST['property_tax_arrears_lakh']) ? (float)$_POST['property_tax_arrears_lakh'] : "NULL";
    $property_tax_gis_linked = mysqli_real_escape_string($con, $_POST['property_tax_gis_linked']);

    /* Health & Education */
    $stray_cattle_count = !empty($_POST['stray_cattle_count']) ? (int)$_POST['stray_cattle_count'] : "NULL";
    $abc_programme_dogs = mysqli_real_escape_string($con, $_POST['abc_programme_dogs']);
    $stray_dogs_sterilised_annual = !empty($_POST['stray_dogs_sterilised_annual']) ? (int)$_POST['stray_dogs_sterilised_annual'] : "NULL";
    $stray_animal_exp_lakh = !empty($_POST['stray_animal_exp_lakh']) ? (float)$_POST['stray_animal_exp_lakh'] : "NULL";
    $health_dispensaries_count = !empty($_POST['health_dispensaries_count']) ? (int)$_POST['health_dispensaries_count'] : "NULL";
    $govt_schools_count = !empty($_POST['govt_schools_count']) ? (int)$_POST['govt_schools_count'] : "NULL";
    $anganwadi_centres_count = !empty($_POST['anganwadi_centres_count']) ? (int)$_POST['anganwadi_centres_count'] : "NULL";
    $anganwadi_in_ulb_building = !empty($_POST['anganwadi_in_ulb_building']) ? (int)$_POST['anganwadi_in_ulb_building'] : "NULL";

    /* Horticulture */
    $parks_count = !empty($_POST['parks_count']) ? (int)$_POST['parks_count'] : "NULL";
    $tree_register_status = mysqli_real_escape_string($con, $_POST['tree_register_status']);
    $tree_count_recorded = !empty($_POST['tree_count_recorded']) ? (int)$_POST['tree_count_recorded'] : "NULL";
    $parks_exp_lakh = !empty($_POST['parks_exp_lakh']) ? (float)$_POST['parks_exp_lakh'] : "NULL";
    $nurseries_count = !empty($_POST['nurseries_count']) ? (int)$_POST['nurseries_count'] : "NULL";

    /* Institutional */
    $revised_taxes_last3yr = mysqli_real_escape_string($con, $_POST['revised_taxes_last3yr']);
    $priority_investment_areas = isset($_POST['priority_investment_areas']) ? mysqli_real_escape_string($con, implode(", ", $_POST['priority_investment_areas'])) : "";
    $ppp_project_experience = mysqli_real_escape_string($con, $_POST['ppp_project_experience']);
    $identified_ppp_projects_detail = mysqli_real_escape_string($con, $_POST['identified_ppp_projects_detail']);
    $sanctioned_posts = !empty($_POST['sanctioned_posts']) ? (int)$_POST['sanctioned_posts'] : "NULL";
    $permanent_employees = !empty($_POST['permanent_employees']) ? (int)$_POST['permanent_employees'] : "NULL";
    $contractual_employees = !empty($_POST['contractual_employees']) ? (int)$_POST['contractual_employees'] : "NULL";
    $vacant_sanctioned_posts = !empty($_POST['vacant_sanctioned_posts']) ? (int)$_POST['vacant_sanctioned_posts'] : "NULL";

    /* Mobility */
    $footpaths_length_km = !empty($_POST['footpaths_length_km']) ? (float)$_POST['footpaths_length_km'] : "NULL";
    $auth_parking_locations = !empty($_POST['auth_parking_locations']) ? (int)$_POST['auth_parking_locations'] : "NULL";
    $parking_vehicle_capacity = !empty($_POST['parking_vehicle_capacity']) ? (int)$_POST['parking_vehicle_capacity'] : "NULL";
    $parking_annual_income_lakh = !empty($_POST['parking_annual_income_lakh']) ? (float)$_POST['parking_annual_income_lakh'] : "NULL";
    $need_additional_parking = mysqli_real_escape_string($con, $_POST['need_additional_parking']);
    $public_bus_service = mysqli_real_escape_string($con, $_POST['public_bus_service']);
    $buses_operating_count = !empty($_POST['buses_operating_count']) ? (int)$_POST['buses_operating_count'] : "NULL";
    $bus_stands_count = !empty($_POST['bus_stands_count']) ? (int)$_POST['bus_stands_count'] : "NULL";
    $ulb_owned_bus_stands = !empty($_POST['ulb_owned_bus_stands']) ? (int)$_POST['ulb_owned_bus_stands'] : "NULL";
    $bus_stand_annual_income_lakh = !empty($_POST['bus_stand_annual_income_lakh']) ? (float)$_POST['bus_stand_annual_income_lakh'] : "NULL";
    $land_near_bus_stand_comm = mysqli_real_escape_string($con, $_POST['land_near_bus_stand_comm']);
    $ev_charging_stations_count = !empty($_POST['ev_charging_stations_count']) ? (int)$_POST['ev_charging_stations_count'] : "NULL";
    $land_for_ev_charging = mysqli_real_escape_string($con, $_POST['land_for_ev_charging']);

    /* Public Amenities */
    $functional_public_toilets = !empty($_POST['functional_public_toilets']) ? (int)$_POST['functional_public_toilets'] : "NULL";
    $cremation_burial_grounds = !empty($_POST['cremation_burial_grounds']) ? (int)$_POST['cremation_burial_grounds'] : "NULL";

    /* Urban Livelihood */
    $registered_street_vendors = !empty($_POST['registered_street_vendors']) ? (int)$_POST['registered_street_vendors'] : "NULL";
    $vending_zones_details = mysqli_real_escape_string($con, $_POST['vending_zones_details']);

    /* WASH */
    $piped_water_coverage_pct = !empty($_POST['piped_water_coverage_pct']) ? (float)$_POST['piped_water_coverage_pct'] : "NULL";
    $water_connections_count = !empty($_POST['water_connections_count']) ? (int)$_POST['water_connections_count'] : "NULL";
    $metered_water_connections = !empty($_POST['metered_water_connections']) ? (int)$_POST['metered_water_connections'] : "NULL";
    $avg_water_supply_hours = !empty($_POST['avg_water_supply_hours']) ? (float)$_POST['avg_water_supply_hours'] : "NULL";
    $non_revenue_water_pct = !empty($_POST['non_revenue_water_pct']) ? (float)$_POST['non_revenue_water_pct'] : "NULL";
    $water_supply_exp_lakh = !empty($_POST['water_supply_exp_lakh']) ? (float)$_POST['water_supply_exp_lakh'] : "NULL";
    $water_pumping_elec_exp_lakh = !empty($_POST['water_pumping_elec_exp_lakh']) ? (float)$_POST['water_pumping_elec_exp_lakh'] : "NULL";
    $sewerage_coverage_pct = !empty($_POST['sewerage_coverage_pct']) ? (float)$_POST['sewerage_coverage_pct'] : "NULL";
    $sewage_generated_mld = !empty($_POST['sewage_generated_mld']) ? (float)$_POST['sewage_generated_mld'] : "NULL";
    $stp_installed_cap_mld = !empty($_POST['stp_installed_cap_mld']) ? (float)$_POST['stp_installed_cap_mld'] : "NULL";
    $actual_sewage_treated_mld = !empty($_POST['actual_sewage_treated_mld']) ? (float)$_POST['actual_sewage_treated_mld'] : "NULL";
    $treated_wastewater_reused = mysqli_real_escape_string($con, $_POST['treated_wastewater_reused']);
    $msw_generated_tpd = !empty($_POST['msw_generated_tpd']) ? (float)$_POST['msw_generated_tpd'] : "NULL";
    $door_to_door_waste_cov_pct = !empty($_POST['door_to_door_waste_cov_pct']) ? (float)$_POST['door_to_door_waste_cov_pct'] : "NULL";
    $waste_segregation_pct = !empty($_POST['waste_segregation_pct']) ? (float)$_POST['waste_segregation_pct'] : "NULL";
    $swm_annual_exp_lakh = !empty($_POST['swm_annual_exp_lakh']) ? (float)$_POST['swm_annual_exp_lakh'] : "NULL";
    $legacy_waste_dumpsite = mysqli_real_escape_string($con, $_POST['legacy_waste_dumpsite']);

    // Fetch Target Town & District details
    $target_ulb_name = $ulb_name;
    $target_district_name = $district_name;
    $target_dist_id = $user_dist_id;
    if ($target_town_id != $user_town_id) {
      $sql_target = "SELECT town_name, district_name, distid FROM towns_streetlight_mapping WHERE town_id = " . (int)$target_town_id;
      $res_target = $con->query($sql_target);
      if ($res_target && $res_target->num_rows > 0) {
        $row_t = $res_target->fetch_assoc();
        $target_ulb_name = $row_t['town_name'];
        $target_district_name = $row_t['district_name'];
        $target_dist_id = $row_t['distid'];
      }
    }

    // Always INSERT a NEW record for every submission
    $query_insert = "INSERT INTO `ulb_user_profile` SET 
      `town_id` = $target_town_id,
      `district_id` = $target_dist_id,
      `ulb_name` = '" . mysqli_real_escape_string($con, $target_ulb_name) . "',
      `district_name` = '" . mysqli_real_escape_string($con, $target_district_name) . "',
      `ulb_type` = '$ulb_type_val',
      `mc_eo_details` = '$mc_eo_details',
      `nodal_officer_details` = '$nodal_officer_details',
      `num_wards` = $num_wards,
      `geo_area_sqkm` = $geo_area_sqkm,
      `population_households` = '$population_households',
      `ulb_known_for` = '$ulb_known_for',
      `top_complaint_services` = '$top_complaint_services',
      `total_properties` = $total_properties,
      `total_land_parcels` = $total_land_parcels,
      `total_land_area_acres` = $total_land_area_acres,
      `vacant_land_parcels` = $vacant_land_parcels,
      `encroached_land_acres` = $encroached_land_acres,
      `road_length_km` = $road_length_km,
      `asset_register_status` = '$asset_register_status',
      `asset_update_freq` = '$asset_update_freq',
      `total_ulb_shops` = $total_ulb_shops,
      `vacant_ulb_shops` = $vacant_ulb_shops,
      `shops_annual_income_lakh` = $shops_annual_income_lakh,
      `municipal_markets_count` = $municipal_markets_count,
      `markets_annual_income_lakh` = $markets_annual_income_lakh,
      `vacant_building_available` = '$vacant_building_available',
      `land_for_comm_dev` = '$land_for_comm_dev',
      `property_for_redev_ppp` = '$property_for_redev_ppp',
      `community_halls_count` = $community_halls_count,
      `sports_grounds_count` = $sports_grounds_count,
      `libraries_count` = $libraries_count,
      `cctv_total_installed` = $cctv_total_installed,
      `cctv_functional` = $cctv_functional,
      `central_control_room` = '$central_control_room',
      `total_streetlights` = $total_streetlights,
      `non_functional_streetlights` = $non_functional_streetlights,
      `streetlights_elec_exp_lakh` = $streetlights_elec_exp_lakh,
      `accounting_system` = '$accounting_system',
      `accounts_prepared_fy` = '$accounts_prepared_fy',
      `accounts_audited_fy` = '$accounts_audited_fy',
      `closing_cash_balance_lakh` = $closing_cash_balance_lakh,
      `total_income_lakh` = $total_income_lakh,
      `total_expenditure_lakh` = $total_expenditure_lakh,
      `own_source_income_lakh` = $own_source_income_lakh,
      `state_grants_lakh` = $state_grants_lakh,
      `central_grants_lakh` = $central_grants_lakh,
      `has_outstanding_loan` = '$has_outstanding_loan',
      `outstanding_loan_lakh` = $outstanding_loan_lakh,
      `has_credit_rating` = '$has_credit_rating',
      `user_charges_collected_services` = '$user_charges_collected_services',
      `revenue_breakdown_json` = '$revenue_breakdown_json',
      `property_tax_registered_count` = $property_tax_registered_count,
      `property_tax_paid_count` = $property_tax_paid_count,
      `property_tax_demand_lakh` = $property_tax_demand_lakh,
      `property_tax_collected_lakh` = $property_tax_collected_lakh,
      `property_tax_arrears_lakh` = $property_tax_arrears_lakh,
      `property_tax_gis_linked` = '$property_tax_gis_linked',
      `stray_cattle_count` = $stray_cattle_count,
      `abc_programme_dogs` = '$abc_programme_dogs',
      `stray_dogs_sterilised_annual` = $stray_dogs_sterilised_annual,
      `stray_animal_exp_lakh` = $stray_animal_exp_lakh,
      `health_dispensaries_count` = $health_dispensaries_count,
      `govt_schools_count` = $govt_schools_count,
      `anganwadi_centres_count` = $anganwadi_centres_count,
      `anganwadi_in_ulb_building` = $anganwadi_in_ulb_building,
      `parks_count` = $parks_count,
      `tree_register_status` = '$tree_register_status',
      `tree_count_recorded` = $tree_count_recorded,
      `parks_exp_lakh` = $parks_exp_lakh,
      `nurseries_count` = $nurseries_count,
      `revised_taxes_last3yr` = '$revised_taxes_last3yr',
      `priority_investment_areas` = '$priority_investment_areas',
      `ppp_project_experience` = '$ppp_project_experience',
      `identified_ppp_projects_detail` = '$identified_ppp_projects_detail',
      `sanctioned_posts` = $sanctioned_posts,
      `permanent_employees` = $permanent_employees,
      `contractual_employees` = $contractual_employees,
      `vacant_sanctioned_posts` = $vacant_sanctioned_posts,
      `footpaths_length_km` = $footpaths_length_km,
      `auth_parking_locations` = $auth_parking_locations,
      `parking_vehicle_capacity` = $parking_vehicle_capacity,
      `parking_annual_income_lakh` = $parking_annual_income_lakh,
      `need_additional_parking` = '$need_additional_parking',
      `public_bus_service` = '$public_bus_service',
      `buses_operating_count` = $buses_operating_count,
      `bus_stands_count` = $bus_stands_count,
      `ulb_owned_bus_stands` = $ulb_owned_bus_stands,
      `bus_stand_annual_income_lakh` = $bus_stand_annual_income_lakh,
      `land_near_bus_stand_comm` = '$land_near_bus_stand_comm',
      `ev_charging_stations_count` = $ev_charging_stations_count,
      `land_for_ev_charging` = '$land_for_ev_charging',
      `functional_public_toilets` = $functional_public_toilets,
      `cremation_burial_grounds` = $cremation_burial_grounds,
      `registered_street_vendors` = $registered_street_vendors,
      `vending_zones_details` = '$vending_zones_details',
      `piped_water_coverage_pct` = $piped_water_coverage_pct,
      `water_connections_count` = $water_connections_count,
      `metered_water_connections` = $metered_water_connections,
      `avg_water_supply_hours` = $avg_water_supply_hours,
      `non_revenue_water_pct` = $non_revenue_water_pct,
      `water_supply_exp_lakh` = $water_supply_exp_lakh,
      `water_pumping_elec_exp_lakh` = $water_pumping_elec_exp_lakh,
      `sewerage_coverage_pct` = $sewerage_coverage_pct,
      `sewage_generated_mld` = $sewage_generated_mld,
      `stp_installed_cap_mld` = $stp_installed_cap_mld,
      `actual_sewage_treated_mld` = $actual_sewage_treated_mld,
      `treated_wastewater_reused` = '$treated_wastewater_reused',
      `msw_generated_tpd` = $msw_generated_tpd,
      `door_to_door_waste_cov_pct` = $door_to_door_waste_cov_pct,
      `waste_segregation_pct` = $waste_segregation_pct,
      `swm_annual_exp_lakh` = $swm_annual_exp_lakh,
      `legacy_waste_dumpsite` = '$legacy_waste_dumpsite',
      `updated_by` = '" . mysqli_real_escape_string($con, $staff_name) . "',
      `status` = 'Submitted',
      `created_at` = NOW()";

    if ($con->query($query_insert)) {
      $insert_id = $con->insert_id;
      $msg = "ULB Profile submission #$insert_id saved successfully! Form has been reset for new entry.";
      $form_submitted_success = true;
    } else {
      $error = "Error saving profile data: " . $con->error;
    }
  }
}

// Load existing saved profile data ONLY if specific ID is passed in GET
$existing_data = array();

if (isset($_GET['id']) && (int)$_GET['id'] > 0) {
  $res_ex = $con->query("SELECT * FROM ulb_user_profile WHERE id = " . (int)$_GET['id']);
  if ($res_ex && $res_ex->num_rows > 0) {
    $existing_data = $res_ex->fetch_assoc();
  }
}

// Helper to echo values safely
function get_val($data, $field, $default = '') {
  return isset($data[$field]) ? htmlspecialchars($data[$field]) : $default;
}

function is_checked_str($data, $field, $val) {
  if (!isset($data[$field])) return '';
  $items = explode(", ", $data[$field]);
  return in_array($val, $items) ? 'checked' : '';
}

function get_rev_val($data, $key) {
  if (empty($data['revenue_breakdown_json'])) return '';
  $arr = json_decode($data['revenue_breakdown_json'], true);
  return isset($arr[$key]) ? htmlspecialchars($arr[$key]) : '';
}
?>
<!DOCTYPE html>
<html lang="en">

<head>
  <title>Know Your ULB - User Profile || PMIDC</title>
  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <style>
    .form-section-title {
      font-size: 1.15rem;
      font-weight: 700;
      color: #0056b3;
      border-bottom: 2px solid #e9ecef;
      padding-bottom: 8px;
      margin-bottom: 20px;
    }
    .nav-tabs .nav-link {
      font-size: 0.85rem;
      font-weight: 600;
      padding: 10px 14px;
      color: #495057;
      border-radius: 4px 4px 0 0;
    }
    .nav-tabs .nav-link.active {
      background-color: #007bff;
      color: #ffffff;
      border-color: #007bff;
    }
    .card-form {
      border: 1px solid #e3e6f0;
      border-radius: 8px;
      box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
    }
    .badge-qno {
      background-color: #e3f2fd;
      color: #0d47a1;
      font-weight: 700;
      padding: 4px 8px;
      border-radius: 4px;
      margin-right: 6px;
    }
    .q-label {
      font-weight: 600;
      color: #2c3e50;
    }
    .req-star {
      color: #dc3545;
      font-weight: bold;
      margin-left: 2px;
    }
    .tab-footer-nav {
      background-color: #f8f9fa;
      border-top: 1px solid #e9ecef;
      padding: 15px;
      margin-top: 25px;
      border-radius: 0 0 6px 6px;
    }
  </style>
</head>

<body>
  <div class="container-scroller">
    <?php include_once('includes/header.php'); ?>

    <nav class="navbar-breadcrumb col-xl-12 col-12 d-flex flex-row p-0">
      <div class="navbar-menu-wrapper d-flex align-items-center justify-content-end">
        <ul class="navbar-nav mr-lg-2">
          <li class="nav-item">
            <div class="d-flex align-items-baseline">
              <p class="mb-0">Home</p>
              <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0">User Profile (Know Your ULB)</p>
            </div>
          </li>
        </ul>
      </div>
    </nav>

    <div class="container-fluid page-body-wrapper">
      <?php include_once('includes/sidebar.php'); ?>

      <div class="main-panel">
        <div class="content-wrapper">

          <?php if (!empty($msg)): ?>
            <div class="alert alert-success alert-dismissible fade show" role="alert">
              <strong>Success!</strong> <?php echo $msg; ?>
              <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                <span aria-hidden="true">&times;</span>
              </button>
            </div>
          <?php endif; ?>

          <?php if (!empty($error)): ?>
            <div class="alert alert-danger alert-dismissible fade show" role="alert">
              <strong>Error!</strong> <?php echo $error; ?>
              <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                <span aria-hidden="true">&times;</span>
              </button>
            </div>
          <?php endif; ?>

          <div class="row">
            <div class="col-12 grid-margin stretch-card">
              <div class="card card-form">
                <div class="card-body">
                  <h4 class="card-title text-primary"><i class="typcn typcn-clipboard"></i> Know Your ULB — 110 Key Questions Profile</h4>
                  <p class="card-description">
                    Comprehensive institutional, financial, operational, and infrastructure survey for <strong><?php echo htmlspecialchars($ulb_name); ?></strong>.
                    Fields marked with <span class="req-star">*</span> are mandatory.
                  </p>

                  <form method="post" id="ulbProfileForm">

                    <?php if ($user_town_id == 0): ?>
                      <!-- State Admin View ULB Selector -->
                      <div class="form-group row bg-light p-3 rounded">
                        <label class="col-sm-3 col-form-label font-weight-bold">Select ULB / Town:</label>
                        <div class="col-sm-6">
                          <select name="selected_town_id" class="form-control">
                            <option value="">-- Select ULB Town --</option>
                            <?php
                            $sql_t = "SELECT town_id, town_name, district_name FROM towns_streetlight_mapping ORDER BY district_name, town_name";
                            $res_t = $con->query($sql_t);
                            while ($r_t = $res_t->fetch_assoc()) {
                              echo "<option value='" . $r_t['town_id'] . "'>" . htmlspecialchars($r_t['district_name'] . " - " . $r_t['town_name']) . "</option>";
                            }
                            ?>
                          </select>
                        </div>
                      </div>
                    <?php endif; ?>

                    <!-- Tabbed Navigation Bar -->
                    <ul class="nav nav-tabs nav-fill mb-4" id="profileTab" role="tablist">
                      <li class="nav-item"><a class="nav-link active" id="basic-tab" data-toggle="tab" href="#basic" role="tab">1. Basic Profile</a></li>
                      <li class="nav-item"><a class="nav-link" id="assets-tab" data-toggle="tab" href="#assets" role="tab">2. Assets & Land</a></li>
                      <li class="nav-item"><a class="nav-link" id="community-tab" data-toggle="tab" href="#community" role="tab">3. Community Infra</a></li>
                      <li class="nav-item"><a class="nav-link" id="digital-tab" data-toggle="tab" href="#digital" role="tab">4. Digital Infra</a></li>
                      <li class="nav-item"><a class="nav-link" id="energy-tab" data-toggle="tab" href="#energy" role="tab">5. Energy</a></li>
                      <li class="nav-item"><a class="nav-link" id="finance-tab" data-toggle="tab" href="#finance" role="tab">6. Finance & Accounts</a></li>
                      <li class="nav-item"><a class="nav-link" id="health-tab" data-toggle="tab" href="#health" role="tab">7. Health & Edu</a></li>
                      <li class="nav-item"><a class="nav-link" id="horticulture-tab" data-toggle="tab" href="#horticulture" role="tab">8. Horticulture</a></li>
                      <li class="nav-item"><a class="nav-link" id="institutional-tab" data-toggle="tab" href="#institutional" role="tab">9. Institutional</a></li>
                      <li class="nav-item"><a class="nav-link" id="mobility-tab" data-toggle="tab" href="#mobility" role="tab">10. Mobility</a></li>
                      <li class="nav-item"><a class="nav-link" id="amenities-tab" data-toggle="tab" href="#amenities" role="tab">11. Public Amenities</a></li>
                      <li class="nav-item"><a class="nav-link" id="livelihood-tab" data-toggle="tab" href="#livelihood" role="tab">12. Livelihood</a></li>
                      <li class="nav-item"><a class="nav-link" id="wash-tab" data-toggle="tab" href="#wash" role="tab">13. WASH</a></li>
                    </ul>

                    <div class="tab-content" id="profileTabContent">

                      <!-- TAB 1: BASIC PROFILE -->
                      <div class="tab-pane fade show active p-3" id="basic" role="tabpanel">
                        <div class="form-section-title">Section 1: Basic Profile</div>
                        <div class="row">
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q1</span> Name of the ULB<span class="req-star">*</span></label>
                            <input type="text" class="form-control" name="ulb_name" value="<?php echo get_val($existing_data, 'ulb_name', $ulb_name); ?>" required readonly>
                          </div>
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q2</span> District<span class="req-star">*</span></label>
                            <input type="text" class="form-control" name="district_name" value="<?php echo get_val($existing_data, 'district_name', $district_name); ?>" required readonly>
                          </div>
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q3</span> Type of ULB<span class="req-star">*</span></label>
                            <select class="form-control" name="ulb_type" required>
                              <?php
                              $types = array("Municipal Corporation", "Municipal Council Class I", "Municipal Council Class II", "Nagar Panchayat", "Other");
                              $cur_t = get_val($existing_data, 'ulb_type', 'Municipal Corporation');
                              foreach ($types as $t) {
                                $sel = ($cur_t == $t) ? 'selected' : '';
                                echo "<option value='$t' $sel>$t</option>";
                              }
                              ?>
                            </select>
                          </div>
                          <div class="col-md-6 form-group">
                            <label class="q-label"><span class="badge-qno">Q4</span> Name & Designation of Municipal Commissioner / EO<span class="req-star">*</span></label>
                            <input type="text" class="form-control" name="mc_eo_details" placeholder="e.g. Sh. Rajesh Sharma, Executive Officer" value="<?php echo get_val($existing_data, 'mc_eo_details'); ?>" required>
                          </div>
                          <div class="col-md-6 form-group">
                            <label class="q-label"><span class="badge-qno">Q5</span> Nodal Officer Name, Designation, Mobile & Email<span class="req-star">*</span></label>
                            <input type="text" class="form-control" name="nodal_officer_details" placeholder="Name, Designation, Mobile, Email" value="<?php echo get_val($existing_data, 'nodal_officer_details'); ?>" required>
                          </div>
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q6</span> Number of Wards<span class="req-star">*</span></label>
                            <input type="number" class="form-control" name="num_wards" placeholder="Enter a number" value="<?php echo get_val($existing_data, 'num_wards'); ?>" required>
                          </div>
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q7</span> Geographical Area (sq. km)<span class="req-star">*</span></label>
                            <input type="number" step="0.01" class="form-control" name="geo_area_sqkm" placeholder="Area in sq km" value="<?php echo get_val($existing_data, 'geo_area_sqkm'); ?>" required>
                          </div>
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q8</span> Population & Households Estimate<span class="req-star">*</span></label>
                            <input type="text" class="form-control" name="population_households" placeholder="e.g. 55,000 pop / 12,000 HH (2024)" value="<?php echo get_val($existing_data, 'population_households'); ?>" required>
                          </div>
                          <div class="col-md-12 form-group">
                            <label class="q-label"><span class="badge-qno">Q9</span> What is the ULB mainly known for? (Select all that apply)</label>
                            <div class="row px-3">
                              <?php
                              $known_options = array("Administrative centre", "Industrial activity", "Agriculture or mandi-related activity", "Trade and commercial activity", "Tourism or heritage", "Religious importance", "Educational institutions", "Healthcare facilities", "Transport or logistics", "Residential or satellite town", "Other");
                              foreach ($known_options as $opt) {
                                $chk = is_checked_str($existing_data, 'ulb_known_for', $opt);
                                echo "<div class='col-md-4 form-check'><label class='form-check-label'><input type='checkbox' class='form-check-input' name='ulb_known_for[]' value='$opt' $chk> $opt</label></div>";
                              }
                              ?>
                            </div>
                          </div>
                          <div class="col-md-12 form-group">
                            <label class="q-label"><span class="badge-qno">Q10</span> Municipal services receiving highest public complaints (Select up to 5)</label>
                            <div class="row px-3">
                              <?php
                              $comp_options = array("Water supply", "Sewerage", "Solid waste collection", "Street sweeping", "Roads", "Streetlights", "Drainage and waterlogging", "Parking", "Public toilets", "Parks", "Property tax", "Building approvals", "Encroachment", "Stray animals", "Other");
                              foreach ($comp_options as $opt) {
                                $chk = is_checked_str($existing_data, 'top_complaint_services', $opt);
                                echo "<div class='col-md-4 form-check'><label class='form-check-label'><input type='checkbox' class='form-check-input' name='top_complaint_services[]' value='$opt' $chk> $opt</label></div>";
                              }
                              ?>
                            </div>
                          </div>
                        </div>
                      </div>

                      <!-- TAB 2: ASSETS & LAND -->
                      <div class="tab-pane fade p-3" id="assets" role="tabpanel">
                        <div class="form-section-title">Section 2: Assets & Land</div>
                        <div class="row">
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q11</span> Total Properties in ULB area<span class="req-star">*</span></label><input type="number" class="form-control" name="total_properties" value="<?php echo get_val($existing_data, 'total_properties'); ?>" required></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q12</span> Total Land Parcels owned by ULB<span class="req-star">*</span></label><input type="number" class="form-control" name="total_land_parcels" value="<?php echo get_val($existing_data, 'total_land_parcels'); ?>" required></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q13</span> Total Area of ULB Land (acres)<span class="req-star">*</span></label><input type="number" step="0.01" class="form-control" name="total_land_area_acres" value="<?php echo get_val($existing_data, 'total_land_area_acres'); ?>" required></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q14</span> Vacant Land Parcels owned<span class="req-star">*</span></label><input type="number" class="form-control" name="vacant_land_parcels" value="<?php echo get_val($existing_data, 'vacant_land_parcels'); ?>" required></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q15</span> Land area under encroachment (acres)</label><input type="number" step="0.01" class="form-control" name="encroached_land_acres" value="<?php echo get_val($existing_data, 'encroached_land_acres'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q16</span> Road length maintained (KMs)<span class="req-star">*</span></label><input type="number" step="0.01" class="form-control" name="road_length_km" value="<?php echo get_val($existing_data, 'road_length_km'); ?>" required></div>
                          <div class="col-md-6 form-group">
                            <label class="q-label"><span class="badge-qno">Q17</span> Asset Register Maintenance</label>
                            <select class="form-control" name="asset_register_status">
                              <option value="Offline" <?php if(get_val($existing_data, 'asset_register_status')=='Offline') echo 'selected'; ?>>Offline</option>
                              <option value="Online (IT system)" <?php if(get_val($existing_data, 'asset_register_status')=='Online (IT system)') echo 'selected'; ?>>Online (IT system)</option>
                            </select>
                          </div>
                          <div class="col-md-6 form-group">
                            <label class="q-label"><span class="badge-qno">Q18</span> Asset Register Update Frequency</label>
                            <select class="form-control" name="asset_update_freq">
                              <option value="Monthly">Monthly</option>
                              <option value="Quarterly">Quarterly</option>
                              <option value="Half-yearly">Half-yearly</option>
                              <option value="Yearly">Yearly</option>
                            </select>
                          </div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q19</span> Total ULB-owned Shops/Commercial Units</label><input type="number" class="form-control" name="total_ulb_shops" value="<?php echo get_val($existing_data, 'total_ulb_shops'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q20</span> Vacant ULB Shops/Units</label><input type="number" class="form-control" name="vacant_ulb_shops" value="<?php echo get_val($existing_data, 'vacant_ulb_shops'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q21</span> Annual Income from Shops (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="shops_annual_income_lakh" value="<?php echo get_val($existing_data, 'shops_annual_income_lakh'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q22</span> Municipal Markets owned/managed</label><input type="number" class="form-control" name="municipal_markets_count" value="<?php echo get_val($existing_data, 'municipal_markets_count'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q23</span> Annual Income from Markets (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="markets_annual_income_lakh" value="<?php echo get_val($existing_data, 'markets_annual_income_lakh'); ?>"></div>
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q24</span> Vacant/Underused ULB Building?</label>
                            <select class="form-control" name="vacant_building_available"><option value="No">No</option><option value="Yes">Yes</option></select>
                          </div>
                          <div class="col-md-6 form-group">
                            <label class="q-label"><span class="badge-qno">Q25</span> ULB Land available for Commercial Dev?</label>
                            <select class="form-control" name="land_for_comm_dev"><option value="No">No</option><option value="Yes">Yes</option></select>
                          </div>
                          <div class="col-md-6 form-group">
                            <label class="q-label"><span class="badge-qno">Q26</span> Property suitable for PPP Redevelopment?</label>
                            <select class="form-control" name="property_for_redev_ppp"><option value="No">No</option><option value="Yes">Yes</option></select>
                          </div>
                        </div>
                      </div>

                      <!-- TAB 3: COMMUNITY INFRASTRUCTURE -->
                      <div class="tab-pane fade p-3" id="community" role="tabpanel">
                        <div class="form-section-title">Section 3: Community Infrastructure</div>
                        <div class="row">
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q27</span> Community Halls owned by ULB</label><input type="number" class="form-control" name="community_halls_count" value="<?php echo get_val($existing_data, 'community_halls_count'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q28</span> Sports Grounds / Stadiums maintained</label><input type="number" class="form-control" name="sports_grounds_count" value="<?php echo get_val($existing_data, 'sports_grounds_count'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q29</span> Public Libraries / Reading rooms</label><input type="number" class="form-control" name="libraries_count" value="<?php echo get_val($existing_data, 'libraries_count'); ?>"></div>
                        </div>
                      </div>

                      <!-- TAB 4: DIGITAL INFRASTRUCTURE -->
                      <div class="tab-pane fade p-3" id="digital" role="tabpanel">
                        <div class="form-section-title">Section 4: Digital Infrastructure</div>
                        <div class="row">
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q30</span> Total CCTV Cameras installed</label><input type="number" class="form-control" name="cctv_total_installed" value="<?php echo get_val($existing_data, 'cctv_total_installed'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q31</span> Functional CCTV Cameras</label><input type="number" class="form-control" name="cctv_functional" value="<?php echo get_val($existing_data, 'cctv_functional'); ?>"></div>
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q32</span> Central Control Room available?</label>
                            <select class="form-control" name="central_control_room"><option value="No">No</option><option value="Yes">Yes</option></select>
                          </div>
                        </div>
                      </div>

                      <!-- TAB 5: ENERGY -->
                      <div class="tab-pane fade p-3" id="energy" role="tabpanel">
                        <div class="form-section-title">Section 5: Energy & Streetlighting</div>
                        <div class="row">
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q33</span> Total Streetlights in ULB (LED & Non-LED)<span class="req-star">*</span></label><input type="number" class="form-control" name="total_streetlights" value="<?php echo get_val($existing_data, 'total_streetlights'); ?>" required></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q34</span> Non-functional Streetlights<span class="req-star">*</span></label><input type="number" class="form-control" name="non_functional_streetlights" value="<?php echo get_val($existing_data, 'non_functional_streetlights'); ?>" required></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q35</span> Annual Electricity Exp on Streetlights (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="streetlights_elec_exp_lakh" value="<?php echo get_val($existing_data, 'streetlights_elec_exp_lakh'); ?>"></div>
                        </div>
                      </div>

                      <!-- TAB 6: FINANCE & ACCOUNTS -->
                      <div class="tab-pane fade p-3" id="finance" role="tabpanel">
                        <div class="form-section-title">Section 6: Finance & Accounts</div>
                        <div class="row">
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q36</span> Accounting System Followed</label>
                            <select class="form-control" name="accounting_system">
                              <option value="Double-entry accrual accounting">Double-entry accrual accounting</option>
                              <option value="Double-entry hybrid accounting">Double-entry hybrid accounting</option>
                              <option value="Cash-based accounting">Cash-based accounting</option>
                            </select>
                          </div>
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q37</span> Accounts Prepared up to FY</label>
                            <select class="form-control" name="accounts_prepared_fy"><option value="FY 24-25">FY 24-25</option><option value="FY 23-24">FY 23-24</option><option value="FY 25-26">FY 25-26</option></select>
                          </div>
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q38</span> Accounts Audited up to FY</label>
                            <select class="form-control" name="accounts_audited_fy"><option value="FY 23-24">FY 23-24</option><option value="FY 24-25">FY 24-25</option><option value="FY 22-23">FY 22-23</option></select>
                          </div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q39</span> Closing Cash/Bank Balance as on March 31 (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="closing_cash_balance_lakh" value="<?php echo get_val($existing_data, 'closing_cash_balance_lakh'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q40</span> Total Income Received in Latest FY (₹ Lakh)<span class="req-star">*</span></label><input type="number" step="0.01" class="form-control" name="total_income_lakh" value="<?php echo get_val($existing_data, 'total_income_lakh'); ?>" required></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q41</span> Total Annual Expenditure in Latest FY (₹ Lakh)<span class="req-star">*</span></label><input type="number" step="0.01" class="form-control" name="total_expenditure_lakh" value="<?php echo get_val($existing_data, 'total_expenditure_lakh'); ?>" required></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q42</span> Own-Source Income (Taxes/Fees/Rents) (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="own_source_income_lakh" value="<?php echo get_val($existing_data, 'own_source_income_lakh'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q43</span> State Government Grants Received (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="state_grants_lakh" value="<?php echo get_val($existing_data, 'state_grants_lakh'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q44</span> Central Govt / Finance Commission Grants (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="central_grants_lakh" value="<?php echo get_val($existing_data, 'central_grants_lakh'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q45</span> Outstanding Loan Available?</label><select class="form-control" name="has_outstanding_loan"><option value="No">No</option><option value="Yes">Yes</option></select></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q46</span> Total Outstanding Loan Amount (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="outstanding_loan_lakh" value="<?php echo get_val($existing_data, 'outstanding_loan_lakh'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q47</span> Has Credit Rating Been Obtained?</label><select class="form-control" name="has_credit_rating"><option value="No">No</option><option value="Yes">Yes</option></select></div>
                          
                          <div class="col-md-12 form-group">
                            <label class="q-label"><span class="badge-qno">Q48</span> Services for which User Charges are collected</label>
                            <div class="row px-3">
                              <?php
                              $uc_services = array("Water supply", "Sewerage", "Solid waste collection", "Parking", "Public toilets", "Markets", "Bus stands", "Other", "No user charges collected");
                              foreach ($uc_services as $s) {
                                $chk = is_checked_str($existing_data, 'user_charges_collected_services', $s);
                                echo "<div class='col-md-4 form-check'><label class='form-check-label'><input type='checkbox' class='form-check-input' name='user_charges_collected_services[]' value='$s' $chk> $s</label></div>";
                              }
                              ?>
                            </div>
                          </div>

                          <div class="col-md-12 form-group">
                            <label class="q-label"><span class="badge-qno">Q49</span> Revenue Breakdown by Source during Latest FY (in ₹ Lakh)</label>
                            <div class="row">
                              <?php
                              $rev_sources = array(
                                'water' => 'Water charges', 'sewerage' => 'Sewerage charges', 'swm' => 'Solid waste user charges',
                                'license' => 'Trade licence fees', 'building' => 'Building plan approval fees', 'dev' => 'Development charges',
                                'fire' => 'Fire NOC fees', 'ad' => 'Advertisement fees', 'parking' => 'Parking fees',
                                'rent' => 'Rent from ULB properties', 'market' => 'Municipal market fees', 'slaughter' => 'Slaughterhouse fees',
                                'certificates' => 'Birth & death certificate fees', 'road_cut' => 'Road-cutting charges', 'telecom' => 'Telecom / RoW charges',
                                'vending' => 'Tehbazari & street-vending', 'hall' => 'Community hall charges', 'crematorium' => 'Crematorium charges',
                                'bus' => 'Bus stand fees', 'interest' => 'Interest income', 'penalties' => 'Penalties & compounding fees', 'other' => 'Other own revenue'
                              );
                              foreach ($rev_sources as $k => $label) {
                                $val = get_rev_val($existing_data, $k);
                                echo "<div class='col-md-3 form-group'><label class='small text-muted'>$label</label><input type='text' class='form-control form-control-sm' name='rev[$k]' placeholder='0 or NA' value='$val'></div>";
                              }
                              ?>
                            </div>
                          </div>

                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q50</span> Properties in Property Tax Register</label><input type="number" class="form-control" name="property_tax_registered_count" value="<?php echo get_val($existing_data, 'property_tax_registered_count'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q51</span> Properties Paid Tax in Latest FY</label><input type="number" class="form-control" name="property_tax_paid_count" value="<?php echo get_val($existing_data, 'property_tax_paid_count'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q52</span> Total Property Tax Demand (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="property_tax_demand_lakh" value="<?php echo get_val($existing_data, 'property_tax_demand_lakh'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q53</span> Total Property Tax Collected (₹ Lakh)<span class="req-star">*</span></label><input type="number" step="0.01" class="form-control" name="property_tax_collected_lakh" value="<?php echo get_val($existing_data, 'property_tax_collected_lakh'); ?>" required></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q54</span> Property Tax Arrears Pending (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="property_tax_arrears_lakh" value="<?php echo get_val($existing_data, 'property_tax_arrears_lakh'); ?>"></div>
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q55</span> Property Tax Linked with GIS?</label>
                            <select class="form-control" name="property_tax_gis_linked"><option value="No">No</option><option value="Yes">Yes</option><option value="Ongoing">Ongoing</option></select>
                          </div>
                        </div>
                      </div>

                      <!-- TAB 7: HEALTH & EDUCATION -->
                      <div class="tab-pane fade p-3" id="health" role="tabpanel">
                        <div class="form-section-title">Section 7: Health & Education</div>
                        <div class="row">
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q56</span> Approx. Stray Cattle in ULB Area</label><input type="number" class="form-control" name="stray_cattle_count" value="<?php echo get_val($existing_data, 'stray_cattle_count'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q57</span> Animal Birth Control (ABC) for Dogs?</label><select class="form-control" name="abc_programme_dogs"><option value="No">No</option><option value="Yes">Yes</option></select></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q58</span> Stray Dogs Sterilised in Latest FY</label><input type="number" class="form-control" name="stray_dogs_sterilised_annual" value="<?php echo get_val($existing_data, 'stray_dogs_sterilised_annual'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q59</span> Annual Exp on Stray Animals (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="stray_animal_exp_lakh" value="<?php echo get_val($existing_data, 'stray_animal_exp_lakh'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q60</span> Dispensaries / Health Centres operated</label><input type="number" class="form-control" name="health_dispensaries_count" value="<?php echo get_val($existing_data, 'health_dispensaries_count'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q61</span> Govt Schools in ULB Area</label><input type="number" class="form-control" name="govt_schools_count" value="<?php echo get_val($existing_data, 'govt_schools_count'); ?>"></div>
                          <div class="col-md-6 form-group"><label class="q-label"><span class="badge-qno">Q62</span> Anganwadi Centres in ULB Area</label><input type="number" class="form-control" name="anganwadi_centres_count" value="<?php echo get_val($existing_data, 'anganwadi_centres_count'); ?>"></div>
                          <div class="col-md-6 form-group"><label class="q-label"><span class="badge-qno">Q63</span> Anganwadis in ULB-owned Buildings</label><input type="number" class="form-control" name="anganwadi_in_ulb_building" value="<?php echo get_val($existing_data, 'anganwadi_in_ulb_building'); ?>"></div>
                        </div>
                      </div>

                      <!-- TAB 8: HORTICULTURE -->
                      <div class="tab-pane fade p-3" id="horticulture" role="tabpanel">
                        <div class="form-section-title">Section 8: Horticulture & Parks</div>
                        <div class="row">
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q64</span> Parks Maintained by ULB</label><input type="number" class="form-control" name="parks_count" value="<?php echo get_val($existing_data, 'parks_count'); ?>"></div>
                          <div class="col-md-4 form-group">
                            <label class="q-label"><span class="badge-qno">Q65</span> Tree Register Maintained?</label>
                            <select class="form-control" name="tree_register_status">
                              <option value="No">No</option>
                              <option value="Yes, for all trees">Yes, for all trees</option>
                              <option value="Yes, for some trees">Yes, for some trees</option>
                            </select>
                          </div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q66</span> Approx. Number of Recorded Trees</label><input type="number" class="form-control" name="tree_count_recorded" value="<?php echo get_val($existing_data, 'tree_count_recorded'); ?>"></div>
                          <div class="col-md-6 form-group"><label class="q-label"><span class="badge-qno">Q67</span> Annual Exp on Parks & Trees (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="parks_exp_lakh" value="<?php echo get_val($existing_data, 'parks_exp_lakh'); ?>"></div>
                          <div class="col-md-6 form-group"><label class="q-label"><span class="badge-qno">Q68</span> Plant Nurseries Operated</label><input type="number" class="form-control" name="nurseries_count" value="<?php echo get_val($existing_data, 'nurseries_count'); ?>"></div>
                        </div>
                      </div>

                      <!-- TAB 9: INSTITUTIONAL -->
                      <div class="tab-pane fade p-3" id="institutional" role="tabpanel">
                        <div class="form-section-title">Section 9: Institutional & Staffing</div>
                        <div class="row">
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q69</span> Revised Taxes/Fees in Last 3 Years?</label><select class="form-control" name="revised_taxes_last3yr"><option value="No">No</option><option value="Yes">Yes</option></select></div>
                          <div class="col-md-8 form-group">
                            <label class="q-label"><span class="badge-qno">Q70</span> Priority Areas Needing Most Investment</label>
                            <div class="row px-3">
                              <?php
                              $prio = array("Water supply", "Sewerage", "Solid waste", "Legacy waste", "Roads and drainage", "Parking / Bus stand", "Street lighting", "EV charging", "Municipal markets", "ULB land development", "CCTV & digital systems", "Parks or water bodies");
                              foreach ($prio as $p) {
                                $chk = is_checked_str($existing_data, 'priority_investment_areas', $p);
                                echo "<div class='col-md-4 form-check'><label class='form-check-label'><input type='checkbox' class='form-check-input' name='priority_investment_areas[]' value='$p' $chk> $p</label></div>";
                              }
                              ?>
                            </div>
                          </div>
                          <div class="col-md-6 form-group">
                            <label class="q-label"><span class="badge-qno">Q71</span> Previous PPP Project Experience</label>
                            <select class="form-control" name="ppp_project_experience">
                              <option value="No">No</option>
                              <option value="Yes, currently operational">Yes, currently operational</option>
                              <option value="Yes, but completed">Yes, but completed</option>
                              <option value="Yes, but discontinued">Yes, but discontinued</option>
                            </select>
                          </div>
                          <div class="col-md-6 form-group"><label class="q-label"><span class="badge-qno">Q72</span> Identified Projects Needing PPP / Private Funding</label><input type="text" class="form-control" name="identified_ppp_projects_detail" placeholder="Explain project details..." value="<?php echo get_val($existing_data, 'identified_ppp_projects_detail'); ?>"></div>
                          <div class="col-md-3 form-group"><label class="q-label"><span class="badge-qno">Q73</span> Sanctioned Posts</label><input type="number" class="form-control" name="sanctioned_posts" value="<?php echo get_val($existing_data, 'sanctioned_posts'); ?>"></div>
                          <div class="col-md-3 form-group"><label class="q-label"><span class="badge-qno">Q74</span> Permanent Employees</label><input type="number" class="form-control" name="permanent_employees" value="<?php echo get_val($existing_data, 'permanent_employees'); ?>"></div>
                          <div class="col-md-3 form-group"><label class="q-label"><span class="badge-qno">Q75</span> Contractual / Outsourced Staff</label><input type="number" class="form-control" name="contractual_employees" value="<?php echo get_val($existing_data, 'contractual_employees'); ?>"></div>
                          <div class="col-md-3 form-group"><label class="q-label"><span class="badge-qno">Q76</span> Vacant Sanctioned Posts</label><input type="number" class="form-control" name="vacant_sanctioned_posts" value="<?php echo get_val($existing_data, 'vacant_sanctioned_posts'); ?>"></div>
                        </div>
                      </div>

                      <!-- TAB 10: MOBILITY -->
                      <div class="tab-pane fade p-3" id="mobility" role="tabpanel">
                        <div class="form-section-title">Section 10: Mobility & Transport</div>
                        <div class="row">
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q77</span> Footpaths Length maintained (KMs)</label><input type="number" step="0.01" class="form-control" name="footpaths_length_km" value="<?php echo get_val($existing_data, 'footpaths_length_km'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q78</span> Authorised Parking Locations</label><input type="number" class="form-control" name="auth_parking_locations" value="<?php echo get_val($existing_data, 'auth_parking_locations'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q79</span> Parking Vehicle Capacity</label><input type="number" class="form-control" name="parking_vehicle_capacity" value="<?php echo get_val($existing_data, 'parking_vehicle_capacity'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q80</span> Annual Parking Income (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="parking_annual_income_lakh" value="<?php echo get_val($existing_data, 'parking_annual_income_lakh'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q81</span> Need Additional Parking?</label><select class="form-control" name="need_additional_parking"><option value="No">No</option><option value="Yes">Yes</option></select></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q82</span> Public Bus Service Available?</label><select class="form-control" name="public_bus_service"><option value="No">No</option><option value="Yes">Yes</option></select></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q83</span> Buses Operating in ULB Area</label><input type="number" class="form-control" name="buses_operating_count" value="<?php echo get_val($existing_data, 'buses_operating_count'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q84</span> Bus Stands / Terminals in Area</label><input type="number" class="form-control" name="bus_stands_count" value="<?php echo get_val($existing_data, 'bus_stands_count'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q85</span> Bus Stands Owned by ULB</label><input type="number" class="form-control" name="ulb_owned_bus_stands" value="<?php echo get_val($existing_data, 'ulb_owned_bus_stands'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q86</span> Annual Bus Stand Income (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="bus_stand_annual_income_lakh" value="<?php echo get_val($existing_data, 'bus_stand_annual_income_lakh'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q87</span> Land Near Bus Stand for Commercial Use?</label><select class="form-control" name="land_near_bus_stand_comm"><option value="No">No</option><option value="Yes">Yes</option></select></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q88</span> Public EV Charging Stations Count</label><input type="number" class="form-control" name="ev_charging_stations_count" value="<?php echo get_val($existing_data, 'ev_charging_stations_count'); ?>"></div>
                          <div class="col-md-12 form-group"><label class="q-label"><span class="badge-qno">Q89</span> ULB Land available for EV Charging Stations?</label><select class="form-control" name="land_for_ev_charging"><option value="No">No</option><option value="Yes">Yes</option></select></div>
                        </div>
                      </div>

                      <!-- TAB 11: PUBLIC AMENITIES -->
                      <div class="tab-pane fade p-3" id="amenities" role="tabpanel">
                        <div class="form-section-title">Section 11: Public Amenities</div>
                        <div class="row">
                          <div class="col-md-6 form-group"><label class="q-label"><span class="badge-qno">Q90</span> Functional Public Toilets Count</label><input type="number" class="form-control" name="functional_public_toilets" value="<?php echo get_val($existing_data, 'functional_public_toilets'); ?>"></div>
                          <div class="col-md-6 form-group"><label class="q-label"><span class="badge-qno">Q91</span> Cremation & Burial Grounds Maintained</label><input type="number" class="form-control" name="cremation_burial_grounds" value="<?php echo get_val($existing_data, 'cremation_burial_grounds'); ?>"></div>
                        </div>
                      </div>

                      <!-- TAB 12: URBAN LIVELIHOOD -->
                      <div class="tab-pane fade p-3" id="livelihood" role="tabpanel">
                        <div class="form-section-title">Section 12: Urban Livelihood</div>
                        <div class="row">
                          <div class="col-md-6 form-group"><label class="q-label"><span class="badge-qno">Q92</span> Registered Street Vendors Count</label><input type="number" class="form-control" name="registered_street_vendors" value="<?php echo get_val($existing_data, 'registered_street_vendors'); ?>"></div>
                          <div class="col-md-6 form-group"><label class="q-label"><span class="badge-qno">Q93</span> Designated Vending Zones & Names</label><input type="text" class="form-control" name="vending_zones_details" placeholder="Count & names of 2-3 main vending zones" value="<?php echo get_val($existing_data, 'vending_zones_details'); ?>"></div>
                        </div>
                      </div>

                      <!-- TAB 13: WASH -->
                      <div class="tab-pane fade p-3" id="wash" role="tabpanel">
                        <div class="form-section-title">Section 13: WASH (Water, Sanitation & Hygiene)</div>
                        <div class="row">
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q94</span> Households with Piped Water (%)</label><input type="number" step="0.01" class="form-control" name="piped_water_coverage_pct" placeholder="0 - 100%" value="<?php echo get_val($existing_data, 'piped_water_coverage_pct'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q95</span> Total Water Connections Count</label><input type="number" class="form-control" name="water_connections_count" value="<?php echo get_val($existing_data, 'water_connections_count'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q96</span> Metered Water Connections Count</label><input type="number" class="form-control" name="metered_water_connections" value="<?php echo get_val($existing_data, 'metered_water_connections'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q97</span> Avg Water Supply Duration (hours/day)</label><input type="number" step="0.1" class="form-control" name="avg_water_supply_hours" value="<?php echo get_val($existing_data, 'avg_water_supply_hours'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q98</span> Non-Revenue Water Loss (%)</label><input type="number" step="0.01" class="form-control" name="non_revenue_water_pct" placeholder="0 - 100%" value="<?php echo get_val($existing_data, 'non_revenue_water_pct'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q99</span> Annual Water Supply Exp (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="water_supply_exp_lakh" value="<?php echo get_val($existing_data, 'water_supply_exp_lakh'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q100</span> Annual Water Pumping Elec Exp (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="water_pumping_elec_exp_lakh" value="<?php echo get_val($existing_data, 'water_pumping_elec_exp_lakh'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q101</span> Sewerage Network Coverage (%)</label><input type="number" step="0.01" class="form-control" name="sewerage_coverage_pct" placeholder="0 - 100%" value="<?php echo get_val($existing_data, 'sewerage_coverage_pct'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q102</span> Sewage Generated per day (MLD)</label><input type="number" step="0.01" class="form-control" name="sewage_generated_mld" value="<?php echo get_val($existing_data, 'sewage_generated_mld'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q103</span> Installed STP Capacity (MLD)</label><input type="number" step="0.01" class="form-control" name="stp_installed_cap_mld" value="<?php echo get_val($existing_data, 'stp_installed_cap_mld'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q104</span> Actual Sewage Treated per day (MLD)</label><input type="number" step="0.01" class="form-control" name="actual_sewage_treated_mld" value="<?php echo get_val($existing_data, 'actual_sewage_treated_mld'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q105</span> Treated Wastewater Reused/Sold?</label><select class="form-control" name="treated_wastewater_reused"><option value="No">No</option><option value="Yes">Yes</option></select></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q106</span> Solid Waste Generated per day (Tonnes)</label><input type="number" step="0.01" class="form-control" name="msw_generated_tpd" value="<?php echo get_val($existing_data, 'msw_generated_tpd'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q107</span> Door-to-Door Waste Coverage (%)</label><input type="number" step="0.01" class="form-control" name="door_to_door_waste_cov_pct" placeholder="0 - 100%" value="<?php echo get_val($existing_data, 'door_to_door_waste_cov_pct'); ?>"></div>
                          <div class="col-md-4 form-group"><label class="q-label"><span class="badge-qno">Q108</span> Waste Segregation at Source (%)</label><input type="number" step="0.01" class="form-control" name="waste_segregation_pct" placeholder="0 - 100%" value="<?php echo get_val($existing_data, 'waste_segregation_pct'); ?>"></div>
                          <div class="col-md-6 form-group"><label class="q-label"><span class="badge-qno">Q109</span> Annual Exp on Solid Waste Management (₹ Lakh)</label><input type="number" step="0.01" class="form-control" name="swm_annual_exp_lakh" value="<?php echo get_val($existing_data, 'swm_annual_exp_lakh'); ?>"></div>
                          <div class="col-md-6 form-group"><label class="q-label"><span class="badge-qno">Q110</span> Legacy Waste Dumpsite in ULB Area?</label><select class="form-control" name="legacy_waste_dumpsite"><option value="No">No</option><option value="Yes">Yes</option></select></div>
                        </div>
                      </div>

                    </div>

                    <!-- Bottom Nav & Action Footer -->
                    <div class="tab-footer-nav d-flex justify-content-between align-items-center">
                      <div>
                        <button type="button" class="btn btn-outline-secondary btn-prev font-weight-bold mr-2">
                          <i class="typcn typcn-arrow-left-thick"></i> Previous Section
                        </button>
                        <button type="button" class="btn btn-info btn-next font-weight-bold">
                          Next Section <i class="typcn typcn-arrow-right-thick"></i>
                        </button>
                      </div>

                      <button type="submit" name="save_profile" class="btn btn-primary btn-lg px-5 font-weight-bold">
                        <i class="typcn typcn-device-floppy"></i> Save ULB Profile Data
                      </button>
                    </div>

                  </form>
                </div>
              </div>
            </div>
          </div>

        </div>
        <?php include_once('includes/footer.php'); ?>
      </div>
    </div>
  </div>

  <script src="vendors/js/vendor.bundle.base.js"></script>
  <script src="js/off-canvas.js"></script>
  <script src="js/hoverable-collapse.js"></script>
  <script src="js/template.js"></script>
  <script>
    $(document).ready(function () {
      <?php if ($form_submitted_success): ?>
        // Reset form inputs after successful submission
        document.getElementById('ulbProfileForm').reset();
        // Return to first tab
        $('#profileTab a[href="#basic"]').tab('show');
      <?php endif; ?>

      // Handle Next / Previous tab navigation
      $('.btn-next').click(function () {
        var $active = $('#profileTab .nav-link.active');
        var $next = $active.parent().next().find('.nav-link');
        if ($next.length > 0) {
          $next.tab('show');
          window.scrollTo({ top: 150, behavior: 'smooth' });
        }
      });

      $('.btn-prev').click(function () {
        var $active = $('#profileTab .nav-link.active');
        var $prev = $active.parent().prev().find('.nav-link');
        if ($prev.length > 0) {
          $prev.tab('show');
          window.scrollTo({ top: 150, behavior: 'smooth' });
        }
      });
    });
  </script>
</body>

</html>
