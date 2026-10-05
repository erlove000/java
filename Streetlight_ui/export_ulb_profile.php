<?php
session_start();
include_once('includes/config.php');

if (strlen($_SESSION['aid']) == 0) {
  header('location:index.php');
  exit();
}

$sql_login = "SELECT districtid, townid FROM streetlightlogin WHERE id=" . (int)$_SESSION['aid'];
$res_login = $con->query($sql_login);
$user_town_id = 0;

if ($res_login && $res_login->num_rows > 0) {
  $row_l = $res_login->fetch_assoc();
  $user_town_id = $row_l['townid'];
}

$where_clauses = array();
if ($user_town_id > 0) {
  $where_clauses[] = "p.town_id = " . (int)$user_town_id;
} else {
  if (!empty($_GET['dist_id'])) {
    $where_clauses[] = "p.district_id = " . (int)$_GET['dist_id'];
  }
  if (!empty($_GET['town_id'])) {
    $where_clauses[] = "p.town_id = " . (int)$_GET['town_id'];
  }
}

$where_sql = "";
if (count($where_clauses) > 0) {
  $where_sql = " WHERE " . implode(" AND ", $where_clauses);
}

$filename = "Know_Your_ULB_Profile_Report_" . date('Y-m-d') . ".csv";
header('Content-Type: text/csv; charset=utf-8');
header('Content-Disposition: attachment; filename=' . $filename);

$output = fopen('php://output', 'w');

// Header Row (110+ Columns)
$headers = array(
  'ULB Name', 'District Name', 'Type of ULB', 'MC / EO Details', 'Nodal Officer Details',
  'No. of Wards', 'Geographical Area (sq km)', 'Population & Households', 'ULB Known For', 'Top Public Complaints',
  'Total Properties', 'Total Land Parcels', 'Total Land Area (Acres)', 'Vacant Land Parcels', 'Encroached Land (Acres)',
  'Road Length (KMs)', 'Asset Register Status', 'Asset Update Freq', 'Total ULB Shops', 'Vacant ULB Shops',
  'Shops Income (Lakh)', 'Municipal Markets Count', 'Markets Income (Lakh)', 'Vacant Building Available', 'Land for Comm Dev',
  'Property for PPP Redevelopment', 'Community Halls Count', 'Sports Grounds Count', 'Libraries Count', 'CCTV Total Installed',
  'CCTV Functional', 'Central Control Room', 'Total Streetlights', 'Non-functional Streetlights', 'Streetlights Elec Exp (Lakh)',
  'Accounting System', 'Accounts Prepared FY', 'Accounts Audited FY', 'Closing Cash Balance (Lakh)', 'Total Income (Lakh)',
  'Total Expenditure (Lakh)', 'Own Source Income (Lakh)', 'State Grants (Lakh)', 'Central Grants (Lakh)', 'Outstanding Loan Status',
  'Outstanding Loan Amount (Lakh)', 'Credit Rating Obtained', 'User Charges Services', 'Properties in Tax Register',
  'Properties Paid Tax', 'Property Tax Demand (Lakh)', 'Property Tax Collected (Lakh)', 'Property Tax Arrears (Lakh)',
  'Property Tax GIS Linked', 'Stray Cattle Count', 'ABC Programme for Dogs', 'Stray Dogs Sterilised Annual', 'Stray Animal Exp (Lakh)',
  'Health Dispensaries Count', 'Govt Schools Count', 'Anganwadi Centres Count', 'Anganwadis in ULB Building', 'Parks Count',
  'Tree Register Status', 'Recorded Trees Count', 'Parks Exp (Lakh)', 'Plant Nurseries Count', 'Revised Taxes Last 3 Yrs',
  'Priority Investment Areas', 'PPP Project Experience', 'Identified PPP Projects', 'Sanctioned Posts', 'Permanent Employees',
  'Contractual Employees', 'Vacant Sanctioned Posts', 'Footpaths Length (KMs)', 'Auth Parking Locations', 'Parking Vehicle Capacity',
  'Parking Annual Income (Lakh)', 'Need Additional Parking', 'Public Bus Service', 'Operating Buses Count', 'Bus Stands Count',
  'ULB Owned Bus Stands', 'Bus Stand Income (Lakh)', 'Land Near Bus Stand Comm', 'Public EV Charging Stations', 'Land for EV Charging',
  'Functional Public Toilets', 'Cremation Burial Grounds', 'Registered Street Vendors', 'Vending Zones Details',
  'Piped Water Coverage (%)', 'Water Connections Count', 'Metered Water Connections', 'Avg Water Supply Hours', 'Non Revenue Water Loss (%)',
  'Water Supply Exp (Lakh)', 'Water Pumping Elec Exp (Lakh)', 'Sewerage Coverage (%)', 'Sewage Generated (MLD)', 'STP Installed Cap (MLD)',
  'Actual Sewage Treated (MLD)', 'Treated Wastewater Reused', 'Solid Waste Generated (TPD)', 'Door to Door Waste Cov (%)', 'Waste Segregation (%)',
  'SWM Annual Exp (Lakh)', 'Legacy Waste Dumpsite', 'Updated By', 'Status', 'Last Updated At'
);

fputcsv($output, $headers);

$sql_rows = "SELECT p.* FROM ulb_user_profile p $where_sql ORDER BY p.updated_at DESC";
$res_rows = $con->query($sql_rows);

if ($res_rows && $res_rows->num_rows > 0) {
  while ($row = $res_rows->fetch_assoc()) {
    fputcsv($output, array(
      $row['ulb_name'], $row['district_name'], $row['ulb_type'], $row['mc_eo_details'], $row['nodal_officer_details'],
      $row['num_wards'], $row['geo_area_sqkm'], $row['population_households'], $row['ulb_known_for'], $row['top_complaint_services'],
      $row['total_properties'], $row['total_land_parcels'], $row['total_land_area_acres'], $row['vacant_land_parcels'], $row['encroached_land_acres'],
      $row['road_length_km'], $row['asset_register_status'], $row['asset_update_freq'], $row['total_ulb_shops'], $row['vacant_ulb_shops'],
      $row['shops_annual_income_lakh'], $row['municipal_markets_count'], $row['markets_annual_income_lakh'], $row['vacant_building_available'], $row['land_for_comm_dev'],
      $row['property_for_redev_ppp'], $row['community_halls_count'], $row['sports_grounds_count'], $row['libraries_count'], $row['cctv_total_installed'],
      $row['cctv_functional'], $row['central_control_room'], $row['total_streetlights'], $row['non_functional_streetlights'], $row['streetlights_elec_exp_lakh'],
      $row['accounting_system'], $row['accounts_prepared_fy'], $row['accounts_audited_fy'], $row['closing_cash_balance_lakh'], $row['total_income_lakh'],
      $row['total_expenditure_lakh'], $row['own_source_income_lakh'], $row['state_grants_lakh'], $row['central_grants_lakh'], $row['has_outstanding_loan'],
      $row['outstanding_loan_lakh'], $row['has_credit_rating'], $row['user_charges_collected_services'], $row['property_tax_registered_count'],
      $row['property_tax_paid_count'], $row['property_tax_demand_lakh'], $row['property_tax_collected_lakh'], $row['property_tax_arrears_lakh'],
      $row['property_tax_gis_linked'], $row['stray_cattle_count'], $row['abc_programme_dogs'], $row['stray_dogs_sterilised_annual'], $row['stray_animal_exp_lakh'],
      $row['health_dispensaries_count'], $row['govt_schools_count'], $row['anganwadi_centres_count'], $row['anganwadi_in_ulb_building'], $row['parks_count'],
      $row['tree_register_status'], $row['tree_count_recorded'], $row['parks_exp_lakh'], $row['nurseries_count'], $row['revised_taxes_last3yr'],
      $row['priority_investment_areas'], $row['ppp_project_experience'], $row['identified_ppp_projects_detail'], $row['sanctioned_posts'], $row['permanent_employees'],
      $row['contractual_employees'], $row['vacant_sanctioned_posts'], $row['footpaths_length_km'], $row['auth_parking_locations'], $row['parking_vehicle_capacity'],
      $row['parking_annual_income_lakh'], $row['need_additional_parking'], $row['public_bus_service'], $row['buses_operating_count'], $row['bus_stands_count'],
      $row['ulb_owned_bus_stands'], $row['bus_stand_annual_income_lakh'], $row['land_near_bus_stand_comm'], $row['ev_charging_stations_count'], $row['land_for_ev_charging'],
      $row['functional_public_toilets'], $row['cremation_burial_grounds'], $row['registered_street_vendors'], $row['vending_zones_details'],
      $row['piped_water_coverage_pct'], $row['water_connections_count'], $row['metered_water_connections'], $row['avg_water_supply_hours'], $row['non_revenue_water_pct'],
      $row['water_supply_exp_lakh'], $row['water_pumping_elec_exp_lakh'], $row['sewerage_coverage_pct'], $row['sewage_generated_mld'], $row['stp_installed_cap_mld'],
      $row['actual_sewage_treated_mld'], $row['treated_wastewater_reused'], $row['msw_generated_tpd'], $row['door_to_door_waste_cov_pct'], $row['waste_segregation_pct'],
      $row['swm_annual_exp_lakh'], $row['legacy_waste_dumpsite'], $row['updated_by'], $row['status'], $row['updated_at']
    ));
  }
}

fclose($output);
exit();
?>
