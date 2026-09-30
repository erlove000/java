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
?>
<!DOCTYPE html>
<html lang="en">

<head>
  <title>ULB Profile Report || PMIDC Know Your ULB</title>
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
    .table-report th { background-color: #f8f9fc; font-weight: 700; color: #4e73df; }
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
              <p class="mb-0">ULB Profile MIS Report</p>
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
          <div class="row">
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
          </div>

          <!-- Filter & Action Header -->
          <div class="row mb-3">
            <div class="col-12">
              <div class="card">
                <div class="card-body py-3">
                  <form method="get" class="form-inline justify-content-between">
                    <div class="form-group mb-0">
                      <h5 class="mb-0 font-weight-bold text-primary"><i class="typcn typcn-chart-bar-outline"></i> Know Your ULB Data Report</h5>
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
                        <i class="typcn typcn-export"></i> Export to Excel
                      </a>
                    </div>
                  </form>
                </div>
              </div>
            </div>
          </div>

          <!-- Report Data Table -->
          <div class="row">
            <div class="col-12 grid-margin stretch-card">
              <div class="card">
                <div class="card-body">
                  <div class="table-responsive">
                    <table class="table table-bordered table-striped table-hover table-report" id="ulbReportTable">
                      <thead>
                        <tr>
                          <th>Sub. ID</th>
                          <th>District</th>
                          <th>ULB Name</th>
                          <th>Type of ULB</th>
                          <th>Nodal Officer / MC Info</th>
                          <th>Wards</th>
                          <th>Land Area (Acres)</th>
                          <th>Streetlights (Total / Defective)</th>
                          <th>Property Tax Collected (₹ Lakh)</th>
                          <th>Submitted Date</th>
                          <th>Action</th>
                        </tr>
                      </thead>
                      <tbody>
                        <?php
                        if ($res_rows && $res_rows->num_rows > 0) {
                          while ($row = $res_rows->fetch_assoc()) {
                        ?>
                            <tr>
                              <td><strong>#<?php echo $row['id']; ?></strong></td>
                              <td><strong><?php echo htmlspecialchars($row['district_name']); ?></strong></td>
                              <td><?php echo htmlspecialchars($row['ulb_name']); ?></td>
                              <td><span class="badge badge-info"><?php echo htmlspecialchars($row['ulb_type']); ?></span></td>
                              <td>
                                <small class="d-block"><strong>MC/EO:</strong> <?php echo htmlspecialchars($row['mc_eo_details']); ?></small>
                                <small class="d-block text-muted"><strong>Nodal:</strong> <?php echo htmlspecialchars($row['nodal_officer_details']); ?></small>
                              </td>
                              <td><?php echo (int)$row['num_wards']; ?></td>
                              <td><?php echo number_format((float)$row['total_land_area_acres'], 2); ?></td>
                              <td>
                                <div>Total: <strong><?php echo (int)$row['total_streetlights']; ?></strong></div>
                                <div class="text-danger"><small>Non-func: <?php echo (int)$row['non_functional_streetlights']; ?></small></div>
                              </td>
                              <td>₹ <?php echo number_format((float)$row['property_tax_collected_lakh'], 2); ?> L</td>
                              <td><small><?php echo date('d-M-Y H:i', strtotime($row['created_at'])); ?></small></td>
                              <td>
                                <a href="user_profile.php?id=<?php echo $row['id']; ?>" class="btn btn-primary btn-sm"><i class="typcn typcn-eye"></i> View Profile Entry</a>
                              </td>
                            </tr>
                        <?php
                          }
                        } else {
                          echo "<tr><td colspan='11' class='text-center py-4 text-muted'>No ULB Profile data submitted yet.</td></tr>";
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
