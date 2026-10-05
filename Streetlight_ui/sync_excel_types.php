<?php
include('includes/config.php');

$updates = [
    // Selects
    ['col' => 'ulb_type', 'type' => 'select', 'opts' => 'Municipal Corporation, Municipal Council Class I, Municipal Council Class II, Nagar Panchayat, Other'],
    ['col' => 'asset_register_status', 'type' => 'select', 'opts' => 'Offline, Online (IT system)'],
    ['col' => 'asset_update_freq', 'type' => 'select', 'opts' => 'Monthly, Quarterly, Half-yearly, Yearly'],
    ['col' => 'vacant_building_available', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'land_for_comm_dev', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'property_for_redev_ppp', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'central_control_room', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'accounting_system', 'type' => 'select', 'opts' => 'Double-entry accrual accounting, Double-entry hybrid accounting, Cash-based accounting'],
    ['col' => 'accounts_prepared_fy', 'type' => 'select', 'opts' => 'FY 23-24, FY 24-25, FY 25-26, Others'],
    ['col' => 'accounts_audited_fy', 'type' => 'select', 'opts' => 'FY 23-24, FY 24-25, FY 25-26, Others'],
    ['col' => 'has_outstanding_loan', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'has_credit_rating', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'property_tax_gis_linked', 'type' => 'select', 'opts' => 'Yes, No, Ongoing'],
    ['col' => 'abc_programme_dogs', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'tree_register_status', 'type' => 'select', 'opts' => 'Yes for all trees, Yes for some trees, No'],
    ['col' => 'revised_taxes_last3yr', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'ppp_project_experience', 'type' => 'select', 'opts' => 'Yes currently operational, Yes but completed, Yes but discontinued, No'],
    ['col' => 'need_additional_parking', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'public_bus_service', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'land_near_bus_stand_comm', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'land_for_ev_charging', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'treated_wastewater_reused', 'type' => 'select', 'opts' => 'Yes, No'],
    ['col' => 'legacy_waste_dumpsite', 'type' => 'select', 'opts' => 'Yes, No'],

    // Checkboxes
    ['col' => 'user_charges_collected_services', 'type' => 'checkbox', 'opts' => 'Water supply, Sewerage, Solid waste collection, Parking, Public toilets, Markets, Bus stands, Other, No user charges are collected'],
    ['col' => 'priority_investment_areas', 'type' => 'checkbox', 'opts' => 'Water supply, Sewerage, Solid waste, Legacy waste, Roads and drainage, Parking, Bus stand, Street lighting, EV charging, Municipal markets, ULB land or property development, CCTV and digital systems, Parks or water bodies, Other']
];

foreach($updates as $u) {
    $col = mysqli_real_escape_string($con, $u['col']);
    $type = mysqli_real_escape_string($con, $u['type']);
    $opts = mysqli_real_escape_string($con, $u['opts']);
    
    $query = "UPDATE survey_questions SET input_type='$type', options_text='$opts' WHERE mapped_column='$col'";
    mysqli_query($con, $query);
    echo "Updated $col to $type<br>";
}
echo "All 25 specific questions have been updated perfectly!";
?>
