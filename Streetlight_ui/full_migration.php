<?php
include('includes/config.php');

echo "<h1>Starting Full Database Migration</h1>";

// 1. Alter survey_questions to support mapped columns
mysqli_query($con, "ALTER TABLE survey_questions ADD COLUMN mapped_column VARCHAR(100) DEFAULT NULL");

// 2. Clear existing dynamic tables
mysqli_query($con, "TRUNCATE TABLE survey_categories");
mysqli_query($con, "TRUNCATE TABLE survey_questions");

// 3. Define the Categories and their Questions with Database mapping
$schema = [
    "Basic Profile" => [
        ["q" => "Name of the ULB", "col" => "ulb_name", "type" => "text", "req" => 1],
        ["q" => "District", "col" => "district_name", "type" => "text", "req" => 1],
        ["q" => "Type of ULB", "col" => "ulb_type", "type" => "text", "req" => 1],
        ["q" => "Name & Designation of Municipal Commissioner / EO", "col" => "mc_eo_details", "type" => "text", "req" => 1],
        ["q" => "Nodal Officer Name, Designation, Mobile & Email", "col" => "nodal_officer_details", "type" => "text", "req" => 1],
        ["q" => "Number of Wards", "col" => "num_wards", "type" => "number", "req" => 1],
        ["q" => "Geographical Area (sq. km)", "col" => "geo_area_sqkm", "type" => "number", "req" => 1],
        ["q" => "Population & Households Estimate", "col" => "population_households", "type" => "text", "req" => 1],
        ["q" => "What is the ULB mainly known for?", "col" => "ulb_known_for", "type" => "text", "req" => 0],
        ["q" => "Municipal services receiving highest public complaints", "col" => "top_complaint_services", "type" => "text", "req" => 0]
    ],
    "Assets & Land" => [
        ["q" => "Total Properties in ULB area", "col" => "total_properties", "type" => "number", "req" => 1],
        ["q" => "Total Land Parcels owned by ULB", "col" => "total_land_parcels", "type" => "number", "req" => 1],
        ["q" => "Total Area of ULB Land (acres)", "col" => "total_land_area_acres", "type" => "number", "req" => 1],
        ["q" => "Vacant Land Parcels owned", "col" => "vacant_land_parcels", "type" => "number", "req" => 1],
        ["q" => "Land area under encroachment (acres)", "col" => "encroached_land_acres", "type" => "number", "req" => 0],
        ["q" => "Total Road length under ULB (km)", "col" => "road_length_km", "type" => "number", "req" => 1],
        ["q" => "Asset Register Status", "col" => "asset_register_status", "type" => "text", "req" => 1],
        ["q" => "How often is the Asset Register updated?", "col" => "asset_update_freq", "type" => "text", "req" => 1],
        ["q" => "Total ULB-owned Shops", "col" => "total_ulb_shops", "type" => "number", "req" => 0],
        ["q" => "Vacant ULB-owned Shops", "col" => "vacant_ulb_shops", "type" => "number", "req" => 0],
        ["q" => "Annual Income from Shops (₹ Lakh)", "col" => "shops_annual_income_lakh", "type" => "number", "req" => 0],
        ["q" => "Number of Municipal Markets", "col" => "municipal_markets_count", "type" => "number", "req" => 0],
        ["q" => "Annual Income from Markets (₹ Lakh)", "col" => "markets_annual_income_lakh", "type" => "number", "req" => 0],
        ["q" => "Vacant building available for new initiatives?", "col" => "vacant_building_available", "type" => "text", "req" => 0],
        ["q" => "Land available for commercial development?", "col" => "land_for_comm_dev", "type" => "text", "req" => 0],
        ["q" => "Property identified for redevelopment via PPP?", "col" => "property_for_redev_ppp", "type" => "text", "req" => 0]
    ],
    "Community Infra" => [
        ["q" => "Number of Community Halls / Dharamshalas", "col" => "community_halls_count", "type" => "number", "req" => 0],
        ["q" => "Number of Sports Grounds / Stadiums", "col" => "sports_grounds_count", "type" => "number", "req" => 0],
        ["q" => "Number of Public Libraries", "col" => "libraries_count", "type" => "number", "req" => 0]
    ],
    "Digital Infra" => [
        ["q" => "Total CCTV Cameras installed by ULB", "col" => "cctv_total_installed", "type" => "number", "req" => 0],
        ["q" => "Functional CCTV Cameras", "col" => "cctv_functional", "type" => "number", "req" => 0],
        ["q" => "Integrated Central Control Room exists?", "col" => "central_control_room", "type" => "text", "req" => 0]
    ],
    "Energy" => [
        ["q" => "Total Streetlights", "col" => "total_streetlights", "type" => "number", "req" => 0],
        ["q" => "Non-functional Streetlights", "col" => "non_functional_streetlights", "type" => "number", "req" => 0],
        ["q" => "Annual Electricity Expenditure on Streetlights (₹ Lakh)", "col" => "streetlights_elec_exp_lakh", "type" => "number", "req" => 0]
    ],
    "Finance & Accounts" => [
        ["q" => "Current Accounting System", "col" => "accounting_system", "type" => "text", "req" => 1],
        ["q" => "Latest FY for which Annual Accounts prepared", "col" => "accounts_prepared_fy", "type" => "text", "req" => 1],
        ["q" => "Latest FY for which Audit completed", "col" => "accounts_audited_fy", "type" => "text", "req" => 1],
        ["q" => "Closing Cash Balance (₹ Lakh)", "col" => "closing_cash_balance_lakh", "type" => "number", "req" => 1],
        ["q" => "Total Annual Income (₹ Lakh)", "col" => "total_income_lakh", "type" => "number", "req" => 1],
        ["q" => "Total Annual Expenditure (₹ Lakh)", "col" => "total_expenditure_lakh", "type" => "number", "req" => 1],
        ["q" => "Own Source Revenue Income (₹ Lakh)", "col" => "own_source_income_lakh", "type" => "number", "req" => 1],
        ["q" => "State Grants / SFC received (₹ Lakh)", "col" => "state_grants_lakh", "type" => "number", "req" => 1],
        ["q" => "Central Grants / CFC received (₹ Lakh)", "col" => "central_grants_lakh", "type" => "number", "req" => 1],
        ["q" => "Has outstanding loans?", "col" => "has_outstanding_loan", "type" => "text", "req" => 1],
        ["q" => "Outstanding Loan Amount (₹ Lakh)", "col" => "outstanding_loan_lakh", "type" => "number", "req" => 0],
        ["q" => "ULB Credit Rating exists?", "col" => "has_credit_rating", "type" => "text", "req" => 1],
        ["q" => "Services for which User Charges are collected", "col" => "user_charges_collected_services", "type" => "text", "req" => 0],
        ["q" => "Property Tax: Number of Registered Properties", "col" => "property_tax_registered_count", "type" => "number", "req" => 1],
        ["q" => "Property Tax: Number of Properties Paying Tax", "col" => "property_tax_paid_count", "type" => "number", "req" => 1],
        ["q" => "Property Tax: Total Demand (₹ Lakh)", "col" => "property_tax_demand_lakh", "type" => "number", "req" => 1],
        ["q" => "Property Tax: Total Collected (₹ Lakh)", "col" => "property_tax_collected_lakh", "type" => "number", "req" => 1],
        ["q" => "Property Tax: Total Arrears (₹ Lakh)", "col" => "property_tax_arrears_lakh", "type" => "number", "req" => 1],
        ["q" => "Is Property Tax system GIS linked?", "col" => "property_tax_gis_linked", "type" => "text", "req" => 1]
    ],
    "Health & Edu" => [
        ["q" => "Estimated Stray Cattle", "col" => "stray_cattle_count", "type" => "number", "req" => 0],
        ["q" => "Animal Birth Control (ABC) programme for dogs active?", "col" => "abc_programme_dogs", "type" => "text", "req" => 0],
        ["q" => "Stray Dogs sterilised annually", "col" => "stray_dogs_sterilised_annual", "type" => "number", "req" => 0],
        ["q" => "Annual Exp on Stray Animal Management (₹ Lakh)", "col" => "stray_animal_exp_lakh", "type" => "number", "req" => 0],
        ["q" => "Number of Health Dispensaries/Clinics owned by ULB", "col" => "health_dispensaries_count", "type" => "number", "req" => 0],
        ["q" => "Number of Govt Schools in ULB area", "col" => "govt_schools_count", "type" => "number", "req" => 0],
        ["q" => "Number of Anganwadi Centres in ULB area", "col" => "anganwadi_centres_count", "type" => "number", "req" => 0],
        ["q" => "Anganwadi Centres operating from ULB buildings", "col" => "anganwadi_in_ulb_building", "type" => "number", "req" => 0]
    ],
    "Horticulture" => [
        ["q" => "Number of Parks maintained", "col" => "parks_count", "type" => "number", "req" => 0],
        ["q" => "Tree Register / Inventory status", "col" => "tree_register_status", "type" => "text", "req" => 0],
        ["q" => "Number of Trees recorded", "col" => "tree_count_recorded", "type" => "number", "req" => 0],
        ["q" => "Annual Exp on Parks & Horticulture (₹ Lakh)", "col" => "parks_exp_lakh", "type" => "number", "req" => 0],
        ["q" => "Number of Nurseries maintained by ULB", "col" => "nurseries_count", "type" => "number", "req" => 0]
    ],
    "Institutional" => [
        ["q" => "Revised/increased any taxes or fees in last 3 years?", "col" => "revised_taxes_last3yr", "type" => "text", "req" => 1],
        ["q" => "Priority areas needing investment (Top 3)", "col" => "priority_investment_areas", "type" => "text", "req" => 0],
        ["q" => "Experience executing PPP projects?", "col" => "ppp_project_experience", "type" => "text", "req" => 1],
        ["q" => "Details of identified potential PPP projects", "col" => "identified_ppp_projects_detail", "type" => "textarea", "req" => 0],
        ["q" => "Total Sanctioned Posts", "col" => "sanctioned_posts", "type" => "number", "req" => 1],
        ["q" => "Working Permanent Employees", "col" => "permanent_employees", "type" => "number", "req" => 1],
        ["q" => "Working Contractual/Outsourced Employees", "col" => "contractual_employees", "type" => "number", "req" => 1],
        ["q" => "Vacant Sanctioned Posts", "col" => "vacant_sanctioned_posts", "type" => "number", "req" => 1]
    ],
    "Mobility" => [
        ["q" => "Length of footpaths constructed (km)", "col" => "footpaths_length_km", "type" => "number", "req" => 0],
        ["q" => "Number of Authorised Parking Locations", "col" => "auth_parking_locations", "type" => "number", "req" => 0],
        ["q" => "Total Parking Vehicle Capacity", "col" => "parking_vehicle_capacity", "type" => "number", "req" => 0],
        ["q" => "Annual Income from Parking (₹ Lakh)", "col" => "parking_annual_income_lakh", "type" => "number", "req" => 0],
        ["q" => "Requirement for additional parking?", "col" => "need_additional_parking", "type" => "text", "req" => 0],
        ["q" => "City Public Bus Service operational?", "col" => "public_bus_service", "type" => "text", "req" => 0],
        ["q" => "Number of Buses operating", "col" => "buses_operating_count", "type" => "number", "req" => 0],
        ["q" => "Total Bus Stands in city", "col" => "bus_stands_count", "type" => "number", "req" => 0],
        ["q" => "Bus Stands owned by ULB", "col" => "ulb_owned_bus_stands", "type" => "number", "req" => 0],
        ["q" => "Annual Income from Bus Stands (₹ Lakh)", "col" => "bus_stand_annual_income_lakh", "type" => "number", "req" => 0],
        ["q" => "Land available near Bus Stand for commercial development?", "col" => "land_near_bus_stand_comm", "type" => "text", "req" => 0],
        ["q" => "Number of EV Charging Stations", "col" => "ev_charging_stations_count", "type" => "number", "req" => 0],
        ["q" => "Land available for setting up EV charging?", "col" => "land_for_ev_charging", "type" => "text", "req" => 0]
    ],
    "Public Amenities" => [
        ["q" => "Functional Public Toilets (PT/CT)", "col" => "functional_public_toilets", "type" => "number", "req" => 0],
        ["q" => "Cremation & Burial Grounds Maintained", "col" => "cremation_burial_grounds", "type" => "number", "req" => 0]
    ],
    "Livelihood" => [
        ["q" => "Registered Street Vendors Count", "col" => "registered_street_vendors", "type" => "number", "req" => 0],
        ["q" => "Designated Vending Zones & Names", "col" => "vending_zones_details", "type" => "text", "req" => 0]
    ],
    "WASH" => [
        ["q" => "Households with Piped Water (%)", "col" => "piped_water_coverage_pct", "type" => "number", "req" => 0],
        ["q" => "Total Water Connections Count", "col" => "water_connections_count", "type" => "number", "req" => 0],
        ["q" => "Metered Water Connections Count", "col" => "metered_water_connections", "type" => "number", "req" => 0],
        ["q" => "Avg Water Supply Duration (hours/day)", "col" => "avg_water_supply_hours", "type" => "number", "req" => 0],
        ["q" => "Non-Revenue Water Loss (%)", "col" => "non_revenue_water_pct", "type" => "number", "req" => 0],
        ["q" => "Annual Water Supply Exp (₹ Lakh)", "col" => "water_supply_exp_lakh", "type" => "number", "req" => 0],
        ["q" => "Annual Water Pumping Elec Exp (₹ Lakh)", "col" => "water_pumping_elec_exp_lakh", "type" => "number", "req" => 0],
        ["q" => "Sewerage Network Coverage (%)", "col" => "sewerage_coverage_pct", "type" => "number", "req" => 0],
        ["q" => "Sewage Generated per day (MLD)", "col" => "sewage_generated_mld", "type" => "number", "req" => 0],
        ["q" => "Installed STP Capacity (MLD)", "col" => "stp_installed_cap_mld", "type" => "number", "req" => 0],
        ["q" => "Actual Sewage Treated per day (MLD)", "col" => "actual_sewage_treated_mld", "type" => "number", "req" => 0],
        ["q" => "Treated Wastewater Reused/Sold?", "col" => "treated_wastewater_reused", "type" => "text", "req" => 0],
        ["q" => "Solid Waste Generated per day (Tonnes)", "col" => "msw_generated_tpd", "type" => "number", "req" => 0],
        ["q" => "Door-to-Door Waste Coverage (%)", "col" => "door_to_door_waste_cov_pct", "type" => "number", "req" => 0],
        ["q" => "Waste Segregation at Source (%)", "col" => "waste_segregation_pct", "type" => "number", "req" => 0],
        ["q" => "Annual Exp on Solid Waste Management (₹ Lakh)", "col" => "swm_annual_exp_lakh", "type" => "number", "req" => 0],
        ["q" => "Legacy Waste Dumpsite in ULB Area?", "col" => "legacy_waste_dumpsite", "type" => "text", "req" => 0]
    ]
];

$c_order = 1;
foreach($schema as $cat_name => $questions) {
    // Insert Category
    $stmt = $con->prepare("INSERT INTO survey_categories (category_name, display_order) VALUES (?, ?)");
    $stmt->bind_param("si", $cat_name, $c_order);
    $stmt->execute();
    $cat_id = $stmt->insert_id;
    echo "Added Category: $cat_name<br>";
    
    // Insert Questions
    $q_order = 1;
    foreach($questions as $q) {
        $stmt_q = $con->prepare("INSERT INTO survey_questions (category_id, question_text, input_type, is_required, display_order, mapped_column) VALUES (?, ?, ?, ?, ?, ?)");
        $stmt_q->bind_param("issiis", $cat_id, $q['q'], $q['type'], $q['req'], $q_order, $q['col']);
        $stmt_q->execute();
        echo " - Added Q: {$q['q']} -> col: {$q['col']}<br>";
        $q_order++;
    }
    $c_order++;
}

echo "<h2>Full Migration Complete! DB is now fully dynamic.</h2>";
?>
