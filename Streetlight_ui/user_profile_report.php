<?php
session_start();
error_reporting(0);
include('includes/config.php');

if (strlen($_SESSION['aid']) == 0) {
  header('location:index.php');
  exit();
}

// Fetch logged in user's town scoping
$sql_login = "SELECT districtid, townid FROM streetlightlogin WHERE id=" . (int)$_SESSION['aid'];
$res_login = $con->query($sql_login);
$user_town_id = 0;
$user_dist_id = 0;

if ($res_login && $res_login->num_rows > 0) {
  $row_l = $res_login->fetch_assoc();
  $user_town_id = $row_l['townid'];
  $user_dist_id = $row_l['districtid'];
}

// Filter handling
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

// Fetch summary metrics
$sql_summary = "SELECT 
  COUNT(*) as total_submitted,
  SUM(num_wards) as sum_wards,
  SUM(total_land_area_acres) as sum_land_acres,
  SUM(property_tax_collected_lakh) as sum_property_tax,
  SUM(total_streetlights) as sum_streetlights
FROM ulb_user_profile p $where_sql";

$res_summary = $con->query($sql_summary);
$summary = $res_summary ? $res_summary->fetch_assoc() : array();

// Fetch report rows
$sql_rows = "SELECT p.*, t.district_name as dist_mapped, t.town_name as town_mapped 
  FROM ulb_user_profile p 
  LEFT JOIN towns_streetlight_mapping t ON p.town_id = t.town_id 
  $where_sql 
  ORDER BY p.created_at DESC";

$res_rows = $con->query($sql_rows);

// Fetch Dynamic Questions
$dyn_questions = [];
$dyn_categories = [];
$sql_dyn_q = "SELECT sq.*, sc.category_name FROM survey_questions sq LEFT JOIN survey_categories sc ON sq.category_id = sc.id WHERE sq.mapped_column IS NULL OR sq.mapped_column = '' ORDER BY sc.display_order ASC, sq.display_order ASC";
$res_dyn_q = $con->query($sql_dyn_q);
if ($res_dyn_q && $res_dyn_q->num_rows > 0) {
    while ($row_q = $res_dyn_q->fetch_assoc()) {
        $dyn_questions[] = $row_q;
        if (!isset($dyn_categories[$row_q['category_id']])) {
            $dyn_categories[$row_q['category_id']] = [
                'name' => $row_q['category_name'] ? $row_q['category_name'] : 'Custom Category',
                'count' => 0
            ];
        }
        $dyn_categories[$row_q['category_id']]['count']++;
    }
}

function fmt($val, $suffix = '') {
  if ($val === NULL || $val === '') return '-';
  return htmlspecialchars($val) . $suffix;
}
?>
<!DOCTYPE html>
<html lang="en">

<head>
  <title>Know Your ULB Data Report || PMIDC</title>
  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <style>
    .metric-card {
      border-radius: 8px;
      color: #fff;
      padding: 18px;
      margin-bottom: 20px;
    }
    .metric-blue { background: linear-gradient(135deg, #1e88e5, #1565c0); }
    .metric-green { background: linear-gradient(135deg, #43a047, #2e7d32); }
    .metric-orange { background: linear-gradient(135deg, #fb8c00, #ef6c00); }
    .metric-purple { background: linear-gradient(135deg, #8e24aa, #6a1b9a); }
    .metric-val { font-size: 1.8rem; font-weight: 700; margin-top: 5px; }

    .table-container-scroll {
      max-height: 700px;
      overflow-x: auto;
      overflow-y: auto;
      border: 1px solid #d1d3e2;
      border-radius: 6px;
    }

    .table-all-cols {
      white-space: nowrap;
      font-size: 0.82rem;
    }

    .table-all-cols thead tr:first-child th {
      position: sticky;
      top: 0;
      z-index: 10;
      font-weight: 700;
      text-transform: uppercase;
      font-size: 0.78rem;
      letter-spacing: 0.5px;
      text-align: center;
      border-bottom: 2px solid #000;
    }

    .table-all-cols thead tr:nth-child(2) th {
      position: sticky;
      top: 35px;
      z-index: 9;
      background-color: #f1f5f9;
      font-weight: 600;
      color: #334155;
    }

    /* Category header colors */
    .cat-basic { background-color: #0284c7; color: #fff; }
    .cat-assets { background-color: #0d9488; color: #fff; }
    .cat-community { background-color: #059669; color: #fff; }
    .cat-digital { background-color: #65a30d; color: #fff; }
    .cat-energy { background-color: #d97706; color: #fff; }
    .cat-finance { background-color: #dc2626; color: #fff; }
    .cat-health { background-color: #e11d48; color: #fff; }
    .cat-horticulture { background-color: #9333ea; color: #fff; }
    .cat-institutional { background-color: #4f46e5; color: #fff; }
    .cat-mobility { background-color: #2563eb; color: #fff; }
    .cat-amenities { background-color: #475569; color: #fff; }
    .cat-livelihood { background-color: #0891b2; color: #fff; }
    .cat-wash { background-color: #15803d; color: #fff; }
    .cat-meta { background-color: #334155; color: #fff; }

    .sticky-col-1 { position: sticky; left: 0; background-color: #fff; z-index: 5; }
    .sticky-col-2 { position: sticky; left: 60px; background-color: #fff; z-index: 5; }
    .sticky-col-3 { position: sticky; left: 160px; background-color: #fff; z-index: 5; }

    tr:hover td.sticky-col-1, tr:hover td.sticky-col-2, tr:hover td.sticky-col-3 {
      background-color: #f1f5f9 !important;
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
              <p class="mb-0">Know Your ULB Data Report</p>
            </div>
          </li>
        </ul>
      </div>
    </nav>

    <div class="container-fluid page-body-wrapper">
      <?php include_once('includes/sidebar.php'); ?>

      <div class="main-panel">
        <div class="content-wrapper">

          <!-- Summary Metric Cards -->
          <!-- <div class="row">
            <div class="col-md-3">
              <div class="metric-card metric-blue shadow-sm">
                <small class="text-uppercase">Total Submissions</small>
                <div class="metric-val"><?php echo number_format((int)$summary['total_submitted']); ?></div>
              </div>
            </div>
            <div class="col-md-3">
              <div class="metric-card metric-green shadow-sm">
                <small class="text-uppercase">Total Municipal Wards</small>
                <div class="metric-val"><?php echo number_format((int)$summary['sum_wards']); ?></div>
              </div>
            </div>
            <div class="col-md-3">
              <div class="metric-card metric-orange shadow-sm">
                <small class="text-uppercase">Total Land Area (Acres)</small>
                <div class="metric-val"><?php echo number_format((float)$summary['sum_land_acres'], 2); ?></div>
              </div>
            </div>
            <div class="col-md-3">
              <div class="metric-card metric-purple shadow-sm">
                <small class="text-uppercase">Property Tax Collected (₹ Lakh)</small>
                <div class="metric-val"><?php echo number_format((float)$summary['sum_property_tax'], 2); ?></div>
              </div>
            </div>
          </div> -->

          <!-- Filter & Action Header -->
          <div class="row mb-3">
            <div class="col-12">
              <div class="card">
                <div class="card-body py-3">
                  <form method="get" class="form-inline justify-content-between">
                    <div class="form-group mb-0">
                      <h5 class="mb-0 font-weight-bold text-primary"><i class="typcn typcn-chart-bar-outline"></i> Know Your ULB Data Report (All 110 Columns)</h5>
                    </div>

                    <div class="d-flex align-items-center">
                      <?php if ($user_town_id == 0): ?>
                        <div class="form-group mx-sm-2 mb-0">
                          <select name="dist_id" class="form-control form-control-sm" onchange="this.form.submit()">
                            <option value="">-- All Districts --</option>
                            <?php
                            $res_d = $con->query("SELECT DISTINCT distid, district_name FROM towns_streetlight_mapping ORDER BY district_name");
                            while ($rd = $res_d->fetch_assoc()) {
                              $sel = ($_GET['dist_id'] == $rd['distid']) ? 'selected' : '';
                              echo "<option value='" . $rd['distid'] . "' $sel>" . htmlspecialchars($rd['district_name']) . "</option>";
                            }
                            ?>
                          </select>
                        </div>
                        <div class="form-group mx-sm-2 mb-0">
                          <select name="town_id" class="form-control form-control-sm" onchange="this.form.submit()">
                            <option value="">-- All ULBs --</option>
                            <?php
                            $res_t = $con->query("SELECT town_id, town_name FROM towns_streetlight_mapping ORDER BY town_name");
                            while ($rt = $res_t->fetch_assoc()) {
                              $sel = ($_GET['town_id'] == $rt['town_id']) ? 'selected' : '';
                              echo "<option value='" . $rt['town_id'] . "' $sel>" . htmlspecialchars($rt['town_name']) . "</option>";
                            }
                            ?>
                          </select>
                        </div>
                      <?php endif; ?>

                      <?php
                      $export_url = "export_ulb_profile.php?dist_id=" . urlencode($_GET['dist_id']) . "&town_id=" . urlencode($_GET['town_id']);
                      ?>
                      <a href="<?php echo $export_url; ?>" class="btn btn-success btn-sm font-weight-bold ml-2">
                        <i class="typcn typcn-export"></i> Export All 110 Columns to Excel
                      </a>
                    </div>
                  </form>
                </div>
              </div>
            </div>
          </div>

          <!-- Comprehensive Data Table with All Columns -->
          <div class="row">
            <div class="col-12 grid-margin stretch-card">
              <div class="card">
                <div class="card-body p-2">
                  <div class="table-container-scroll">
                    <table class="table table-bordered table-striped table-hover table-all-cols mb-0" id="ulbReportTableAll">
                      <thead>
                        <!-- Category Header Row -->
                        <tr>
                          <th colspan="11" class="cat-basic">1. Basic Profile (Q1-Q10)</th>
                          <th colspan="16" class="cat-assets">2. Assets & Land (Q11-Q26)</th>
                          <th colspan="3" class="cat-community">3. Community Infra (Q27-Q29)</th>
                          <th colspan="3" class="cat-digital">4. Digital Infra (Q30-Q32)</th>
                          <th colspan="3" class="cat-energy">5. Energy & Streetlights (Q33-Q35)</th>
                          <th colspan="20" class="cat-finance">6. Finance & Accounts (Q36-Q55)</th>
                          <th colspan="8" class="cat-health">7. Health & Education (Q56-Q63)</th>
                          <th colspan="5" class="cat-horticulture">8. Horticulture & Parks (Q64-Q68)</th>
                          <th colspan="8" class="cat-institutional">9. Institutional & Staffing (Q69-Q76)</th>
                          <th colspan="13" class="cat-mobility">10. Mobility & Transport (Q77-Q89)</th>
                          <th colspan="2" class="cat-amenities">11. Public Amenities (Q90-Q91)</th>
                          <th colspan="2" class="cat-livelihood">12. Urban Livelihood (Q92-Q93)</th>
                          <th colspan="17" class="cat-wash">13. WASH (Q94-Q110)</th>
                          <?php
                          foreach ($dyn_categories as $cat) {
                              echo '<th colspan="'.$cat['count'].'" class="cat-meta" style="background-color:#6366f1;">'.htmlspecialchars($cat['name']).'</th>';
                          }
                          ?>
                          <th colspan="4" class="cat-meta">Submission Info</th>
                        </tr>
                        <!-- Individual Column Names Row -->
                        <tr>
                          <th class="sticky-col-1">Sub ID</th>
                          <th class="sticky-col-2">District</th>
                          <th class="sticky-col-3">ULB Name</th>
                          <th>ULB Type</th>
                          <th>MC / EO Details</th>
                          <th>Nodal Officer Details</th>
                          <th>Wards</th>
                          <th>Area (sq km)</th>
                          <th>Population & HH</th>
                          <th>Known For</th>
                          <th>Top Complaints</th>

                          <!-- Assets & Land -->
                          <th>Total Properties</th>
                          <th>Land Parcels</th>
                          <th>Land Area (Acres)</th>
                          <th>Vacant Parcels</th>
                          <th>Encroached (Acres)</th>
                          <th>Road Length (KMs)</th>
                          <th>Asset Register Status</th>
                          <th>Asset Update Freq</th>
                          <th>ULB Shops</th>
                          <th>Vacant Shops</th>
                          <th>Shops Income (₹ Lakh)</th>
                          <th>Markets</th>
                          <th>Markets Income (₹ Lakh)</th>
                          <th>Vacant Building</th>
                          <th>Land for Comm Dev</th>
                          <th>Property for PPP</th>

                          <!-- Community Infra -->
                          <th>Community Halls</th>
                          <th>Sports Grounds</th>
                          <th>Libraries</th>

                          <!-- Digital Infra -->
                          <th>CCTV Total</th>
                          <th>CCTV Functional</th>
                          <th>Central Control Room</th>

                          <!-- Energy -->
                          <th>Total Streetlights</th>
                          <th>Non-functional Streetlights</th>
                          <th>Streetlight Elec Exp (₹ Lakh)</th>

                          <!-- Finance & Accounts -->
                          <th>Accounting System</th>
                          <th>Accounts Prepared FY</th>
                          <th>Accounts Audited FY</th>
                          <th>Cash Balance (₹ Lakh)</th>
                          <th>Total Income (₹ Lakh)</th>
                          <th>Expenditure (₹ Lakh)</th>
                          <th>Own Source Income (₹ Lakh)</th>
                          <th>State Grants (₹ Lakh)</th>
                          <th>Central Grants (₹ Lakh)</th>
                          <th>Outstanding Loan</th>
                          <th>Loan Amount (₹ Lakh)</th>
                          <th>Credit Rating</th>
                          <th>User Charges Services</th>
                          <th>Tax Registered Properties</th>
                          <th>Tax Paid Properties</th>
                          <th>Tax Demand (₹ Lakh)</th>
                          <th>Tax Collected (₹ Lakh)</th>
                          <th>Tax Arrears (₹ Lakh)</th>
                          <th>Tax GIS Linked</th>

                          <!-- Health & Edu -->
                          <th>Stray Cattle</th>
                          <th>ABC Dogs</th>
                          <th>Stray Dogs Sterilised</th>
                          <th>Stray Animal Exp (₹ Lakh)</th>
                          <th>Dispensaries</th>
                          <th>Govt Schools</th>
                          <th>Anganwadis</th>
                          <th>Anganwadis in ULB Bldg</th>

                          <!-- Horticulture -->
                          <th>Parks</th>
                          <th>Tree Register</th>
                          <th>Recorded Trees</th>
                          <th>Parks Exp (₹ Lakh)</th>
                          <th>Nurseries</th>

                          <!-- Institutional -->
                          <th>Revised Taxes Last 3 Yrs</th>
                          <th>Priority Investment Areas</th>
                          <th>PPP Experience</th>
                          <th>Identified PPP Projects</th>
                          <th>Sanctioned Posts</th>
                          <th>Permanent Employees</th>
                          <th>Contractual Staff</th>
                          <th>Vacant Posts</th>

                          <!-- Mobility -->
                          <th>Footpaths (KMs)</th>
                          <th>Parking Locations</th>
                          <th>Parking Capacity</th>
                          <th>Parking Income (₹ Lakh)</th>
                          <th>Need Additional Parking</th>
                          <th>Bus Service</th>
                          <th>Operating Buses</th>
                          <th>Bus Stands</th>
                          <th>ULB Bus Stands</th>
                          <th>Bus Stand Income (₹ Lakh)</th>
                          <th>Land Near Bus Stand Comm</th>
                          <th>EV Charging Stations</th>
                          <th>Land for EV Charging</th>

                          <!-- Public Amenities -->
                          <th>Public Toilets</th>
                          <th>Cremation Grounds</th>

                          <!-- Urban Livelihood -->
                          <th>Street Vendors</th>
                          <th>Vending Zones Details</th>

                          <!-- WASH -->
                          <th>Piped Water Coverage (%)</th>
                          <th>Water Connections</th>
                          <th>Metered Connections</th>
                          <th>Avg Water Hours</th>
                          <th>Non-Revenue Water (%)</th>
                          <th>Water Supply Exp (₹ Lakh)</th>
                          <th>Water Elec Exp (₹ Lakh)</th>
                          <th>Sewerage Coverage (%)</th>
                          <th>Sewage Generated (MLD)</th>
                          <th>STP Installed Cap (MLD)</th>
                          <th>Actual Sewage Treated (MLD)</th>
                          <th>Treated Water Reused</th>
                          <th>Solid Waste (TPD)</th>
                          <th>Door-to-Door Waste (%)</th>
                          <th>Waste Segregation (%)</th>
                          <th>SWM Exp (₹ Lakh)</th>
                          <th>Legacy Waste Dumpsite</th>

                          <!-- Dynamic Questions -->
                          <?php
                          foreach ($dyn_questions as $dq) {
                              echo '<th>'.htmlspecialchars($dq['question_text']).'</th>';
                          }
                          ?>
                          <!-- Metadata -->
                          <th>Updated By</th>
                          <th>Status</th>
                          <th>Submitted Date</th>
                          <th>Action</th>
                        </tr>
                      </thead>
                      <tbody>
                        <?php
                        if ($res_rows && $res_rows->num_rows > 0) {
                          while ($row = $res_rows->fetch_assoc()) {
                              // Fetch dynamic answers for this town
                              $town_ans = [];
                              $sql_ans = "SELECT question_id, answer_text FROM survey_answers WHERE town_id = ".$row['town_id'];
                              $res_ans = $con->query($sql_ans);
                              if ($res_ans) {
                                  while ($a_row = $res_ans->fetch_assoc()) {
                                      $town_ans[$a_row['question_id']] = $a_row['answer_text'];
                                  }
                              }
                        ?>
                            <tr>
                              <td class="sticky-col-1 font-weight-bold">#<?php echo $row['id']; ?></td>
                              <td class="sticky-col-2 font-weight-bold"><?php echo htmlspecialchars($row['district_name']); ?></td>
                              <td class="sticky-col-3 font-weight-bold text-primary"><?php echo htmlspecialchars($row['ulb_name']); ?></td>
                              <td><span class="badge badge-info"><?php echo fmt($row['ulb_type']); ?></span></td>
                              <td><?php echo fmt($row['mc_eo_details']); ?></td>
                              <td><?php echo fmt($row['nodal_officer_details']); ?></td>
                              <td><?php echo fmt($row['num_wards']); ?></td>
                              <td><?php echo fmt($row['geo_area_sqkm']); ?></td>
                              <td><?php echo fmt($row['population_households']); ?></td>
                              <td><?php echo fmt($row['ulb_known_for']); ?></td>
                              <td><?php echo fmt($row['top_complaint_services']); ?></td>

                              <!-- Assets & Land -->
                              <td><?php echo fmt($row['total_properties']); ?></td>
                              <td><?php echo fmt($row['total_land_parcels']); ?></td>
                              <td><?php echo fmt($row['total_land_area_acres']); ?></td>
                              <td><?php echo fmt($row['vacant_land_parcels']); ?></td>
                              <td><?php echo fmt($row['encroached_land_acres']); ?></td>
                              <td><?php echo fmt($row['road_length_km']); ?></td>
                              <td><?php echo fmt($row['asset_register_status']); ?></td>
                              <td><?php echo fmt($row['asset_update_freq']); ?></td>
                              <td><?php echo fmt($row['total_ulb_shops']); ?></td>
                              <td><?php echo fmt($row['vacant_ulb_shops']); ?></td>
                              <td><?php echo fmt($row['shops_annual_income_lakh']); ?></td>
                              <td><?php echo fmt($row['municipal_markets_count']); ?></td>
                              <td><?php echo fmt($row['markets_annual_income_lakh']); ?></td>
                              <td><?php echo fmt($row['vacant_building_available']); ?></td>
                              <td><?php echo fmt($row['land_for_comm_dev']); ?></td>
                              <td><?php echo fmt($row['property_for_redev_ppp']); ?></td>

                              <!-- Community Infra -->
                              <td><?php echo fmt($row['community_halls_count']); ?></td>
                              <td><?php echo fmt($row['sports_grounds_count']); ?></td>
                              <td><?php echo fmt($row['libraries_count']); ?></td>

                              <!-- Digital Infra -->
                              <td><?php echo fmt($row['cctv_total_installed']); ?></td>
                              <td><?php echo fmt($row['cctv_functional']); ?></td>
                              <td><?php echo fmt($row['central_control_room']); ?></td>

                              <!-- Energy -->
                              <td><?php echo fmt($row['total_streetlights']); ?></td>
                              <td><span class="text-danger font-weight-bold"><?php echo fmt($row['non_functional_streetlights']); ?></span></td>
                              <td><?php echo fmt($row['streetlights_elec_exp_lakh']); ?></td>

                              <!-- Finance & Accounts -->
                              <td><?php echo fmt($row['accounting_system']); ?></td>
                              <td><?php echo fmt($row['accounts_prepared_fy']); ?></td>
                              <td><?php echo fmt($row['accounts_audited_fy']); ?></td>
                              <td><?php echo fmt($row['closing_cash_balance_lakh']); ?></td>
                              <td><?php echo fmt($row['total_income_lakh']); ?></td>
                              <td><?php echo fmt($row['total_expenditure_lakh']); ?></td>
                              <td><?php echo fmt($row['own_source_income_lakh']); ?></td>
                              <td><?php echo fmt($row['state_grants_lakh']); ?></td>
                              <td><?php echo fmt($row['central_grants_lakh']); ?></td>
                              <td><?php echo fmt($row['has_outstanding_loan']); ?></td>
                              <td><?php echo fmt($row['outstanding_loan_lakh']); ?></td>
                              <td><?php echo fmt($row['has_credit_rating']); ?></td>
                              <td><?php echo fmt($row['user_charges_collected_services']); ?></td>
                              <td><?php echo fmt($row['property_tax_registered_count']); ?></td>
                              <td><?php echo fmt($row['property_tax_paid_count']); ?></td>
                              <td><?php echo fmt($row['property_tax_demand_lakh']); ?></td>
                              <td><strong class="text-success">₹ <?php echo fmt($row['property_tax_collected_lakh']); ?> L</strong></td>
                              <td><?php echo fmt($row['property_tax_arrears_lakh']); ?></td>
                              <td><?php echo fmt($row['property_tax_gis_linked']); ?></td>

                              <!-- Health & Edu -->
                              <td><?php echo fmt($row['stray_cattle_count']); ?></td>
                              <td><?php echo fmt($row['abc_programme_dogs']); ?></td>
                              <td><?php echo fmt($row['stray_dogs_sterilised_annual']); ?></td>
                              <td><?php echo fmt($row['stray_animal_exp_lakh']); ?></td>
                              <td><?php echo fmt($row['health_dispensaries_count']); ?></td>
                              <td><?php echo fmt($row['govt_schools_count']); ?></td>
                              <td><?php echo fmt($row['anganwadi_centres_count']); ?></td>
                              <td><?php echo fmt($row['anganwadi_in_ulb_building']); ?></td>

                              <!-- Horticulture -->
                              <td><?php echo fmt($row['parks_count']); ?></td>
                              <td><?php echo fmt($row['tree_register_status']); ?></td>
                              <td><?php echo fmt($row['tree_count_recorded']); ?></td>
                              <td><?php echo fmt($row['parks_exp_lakh']); ?></td>
                              <td><?php echo fmt($row['nurseries_count']); ?></td>

                              <!-- Institutional -->
                              <td><?php echo fmt($row['revised_taxes_last3yr']); ?></td>
                              <td><?php echo fmt($row['priority_investment_areas']); ?></td>
                              <td><?php echo fmt($row['ppp_project_experience']); ?></td>
                              <td><?php echo fmt($row['identified_ppp_projects_detail']); ?></td>
                              <td><?php echo fmt($row['sanctioned_posts']); ?></td>
                              <td><?php echo fmt($row['permanent_employees']); ?></td>
                              <td><?php echo fmt($row['contractual_employees']); ?></td>
                              <td><?php echo fmt($row['vacant_sanctioned_posts']); ?></td>

                              <!-- Mobility -->
                              <td><?php echo fmt($row['footpaths_length_km']); ?></td>
                              <td><?php echo fmt($row['auth_parking_locations']); ?></td>
                              <td><?php echo fmt($row['parking_vehicle_capacity']); ?></td>
                              <td><?php echo fmt($row['parking_annual_income_lakh']); ?></td>
                              <td><?php echo fmt($row['need_additional_parking']); ?></td>
                              <td><?php echo fmt($row['public_bus_service']); ?></td>
                              <td><?php echo fmt($row['buses_operating_count']); ?></td>
                              <td><?php echo fmt($row['bus_stands_count']); ?></td>
                              <td><?php echo fmt($row['ulb_owned_bus_stands']); ?></td>
                              <td><?php echo fmt($row['bus_stand_annual_income_lakh']); ?></td>
                              <td><?php echo fmt($row['land_near_bus_stand_comm']); ?></td>
                              <td><?php echo fmt($row['ev_charging_stations_count']); ?></td>
                              <td><?php echo fmt($row['land_for_ev_charging']); ?></td>

                              <!-- Public Amenities -->
                              <td><?php echo fmt($row['functional_public_toilets']); ?></td>
                              <td><?php echo fmt($row['cremation_burial_grounds']); ?></td>

                              <!-- Urban Livelihood -->
                              <td><?php echo fmt($row['registered_street_vendors']); ?></td>
                              <td><?php echo fmt($row['vending_zones_details']); ?></td>

                              <!-- WASH -->
                              <td><?php echo fmt($row['piped_water_coverage_pct'], '%'); ?></td>
                              <td><?php echo fmt($row['water_connections_count']); ?></td>
                              <td><?php echo fmt($row['metered_water_connections']); ?></td>
                              <td><?php echo fmt($row['avg_water_supply_hours']); ?></td>
                              <td><?php echo fmt($row['non_revenue_water_pct'], '%'); ?></td>
                              <td><?php echo fmt($row['water_supply_exp_lakh']); ?></td>
                              <td><?php echo fmt($row['water_pumping_elec_exp_lakh']); ?></td>
                              <td><?php echo fmt($row['sewerage_coverage_pct'], '%'); ?></td>
                              <td><?php echo fmt($row['sewage_generated_mld']); ?></td>
                              <td><?php echo fmt($row['stp_installed_cap_mld']); ?></td>
                              <td><?php echo fmt($row['actual_sewage_treated_mld']); ?></td>
                              <td><?php echo fmt($row['treated_wastewater_reused']); ?></td>
                              <td><?php echo fmt($row['msw_generated_tpd']); ?></td>
                              <td><?php echo fmt($row['door_to_door_waste_cov_pct'], '%'); ?></td>
                              <td><?php echo fmt($row['waste_segregation_pct'], '%'); ?></td>
                              <td><?php echo fmt($row['swm_annual_exp_lakh']); ?></td>
                              <td><?php echo fmt($row['legacy_waste_dumpsite']); ?></td>

                              <!-- Dynamic Answers -->
                              <?php
                              foreach ($dyn_questions as $dq) {
                                  $ans_val = isset($town_ans[$dq['id']]) ? $town_ans[$dq['id']] : '-';
                                  echo '<td>'.fmt($ans_val).'</td>';
                              }
                              ?>

                              <!-- Metadata -->
                              <td><?php echo fmt($row['updated_by']); ?></td>
                              <td><span class="badge badge-success"><?php echo fmt($row['status']); ?></span></td>
                              <td><small><?php echo date('d-M-Y H:i', strtotime($row['created_at'])); ?></small></td>
                              <td>
                                <a href="user_profile.php?id=<?php echo $row['id']; ?>" class="btn btn-primary btn-xs"><i class="typcn typcn-eye"></i> View</a>
                              </td>
                            </tr>
                        <?php
                          }
                        } else {
                          echo "<tr><td colspan='118' class='text-center py-4 text-muted'>No ULB Profile data submitted yet.</td></tr>";
                        }
                        ?>
                      </tbody>
                    </table>
                  </div>
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
</body>

</html>
