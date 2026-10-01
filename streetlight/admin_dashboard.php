<?php
session_start();
error_reporting(0);
include('includes/config.php');

if (strlen($_SESSION['aid']) == 0) {
  header('location:index.php');
  exit();
}

// Ensure login is State Admin (townid = 0)
$sql_login = "SELECT districtid, townid, staff_name FROM streetlightlogin WHERE id=" . (int)$_SESSION['aid'];
$res_login = $con->query($sql_login);
$admin_town_id = 0;
$staff_name = "Administrator";

if ($res_login && $res_login->num_rows > 0) {
  $row_l = $res_login->fetch_assoc();
  $admin_town_id = $row_l['townid'];
  if (!empty($row_l['staff_name'])) {
    $staff_name = $row_l['staff_name'];
  }
}

// Redirect non-admin users (townid != 0) back to main dashboard
if ($admin_town_id != 0) {
  header('location:dashboard.php');
  exit();
}

// Fetch Master Statistics
$count_ulbs = 0;
$res_ulb = $con->query("SELECT COUNT(*) as cnt, SUM(target_value) as total_target FROM towns_streetlight_mapping");
if ($res_ulb && $r = $res_ulb->fetch_assoc()) {
  $count_ulbs = $r['cnt'];
  $total_target_lights = $r['total_target'];
}

$count_users = 0;
$res_usr = $con->query("SELECT COUNT(*) as cnt FROM streetlightlogin");
if ($res_usr && $r = $res_usr->fetch_assoc()) {
  $count_users = $r['cnt'];
}

$count_profiles = 0;
$res_prof = $con->query("SELECT COUNT(*) as cnt FROM ulb_user_profile");
if ($res_prof && $r = $res_prof->fetch_assoc()) {
  $count_profiles = $r['cnt'];
}

$count_logs_today = 0;
$res_logs = $con->query("SELECT COUNT(*) as cnt FROM streetlightdata WHERE DATE(DOA) = CURDATE()");
if ($res_logs && $r = $res_logs->fetch_assoc()) {
  $count_logs_today = $r['cnt'];
}
?>
<!DOCTYPE html>
<html lang="en">

<head>
  <title>Master Admin Control Dashboard || PMIDC</title>
  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <style>
    .admin-card {
      border-radius: 10px;
      color: #fff;
      padding: 22px;
      margin-bottom: 25px;
      box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
      transition: transform 0.2s ease;
    }
    .admin-card:hover { transform: translateY(-4px); }
    .card-purple { background: linear-gradient(135deg, #6b21a8, #4c1d95); }
    .card-blue { background: linear-gradient(135deg, #0284c7, #0369a1); }
    .card-emerald { background: linear-gradient(135deg, #059669, #047857); }
    .card-amber { background: linear-gradient(135deg, #d97706, #b45309); }
    
    .card-val { font-size: 2.2rem; font-weight: 800; margin-top: 6px; }
    
    .widget-box {
      background: #fff;
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      padding: 20px;
      margin-bottom: 20px;
      box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);
    }
    .widget-icon {
      font-size: 2.5rem;
      color: #0284c7;
      margin-bottom: 10px;
    }
    .widget-title { font-weight: 700; color: #1e293b; font-size: 1.1rem; }
    .widget-desc { color: #64748b; font-size: 0.88rem; margin-bottom: 15px; }
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
              <p class="mb-0">Master Admin Dashboard</p>
            </div>
          </li>
        </ul>
      </div>
    </nav>

    <div class="container-fluid page-body-wrapper">
      <?php include_once('includes/sidebar.php'); ?>

      <div class="main-panel">
        <div class="content-wrapper">

          <!-- Welcome Banner -->
          <div class="row mb-4">
            <div class="col-12">
              <div class="card bg-primary text-white shadow">
                <div class="card-body py-4 d-flex justify-content-between align-items-center">
                  <div>
                    <h3 class="font-weight-bold mb-1"><i class="typcn typcn-cog"></i> Master System Administration Control</h3>
                    <p class="mb-0 opacity-80">Logged in as <strong><?php echo htmlspecialchars($staff_name); ?></strong> (State Administrator)</p>
                  </div>
                  <span class="badge badge-light text-primary px-3 py-2 font-weight-bold">State Level Scoped</span>
                </div>
              </div>
            </div>
          </div>

          <!-- Top Metric Cards -->
          <div class="row">
            <div class="col-md-3">
              <div class="admin-card card-purple">
                <small class="text-uppercase font-weight-bold">Registered ULB Towns</small>
                <div class="card-val"><?php echo number_format($count_ulbs); ?></div>
                <small class="opacity-80">Target Points: <?php echo number_format($total_target_lights); ?></small>
              </div>
            </div>
            <div class="col-md-3">
              <div class="admin-card card-blue">
                <small class="text-uppercase font-weight-bold">User Login Accounts</small>
                <div class="card-val"><?php echo number_format($count_users); ?></div>
                <small class="opacity-80">Active Staff & Admins</small>
              </div>
            </div>
            <div class="col-md-3">
              <div class="admin-card card-emerald">
                <small class="text-uppercase font-weight-bold">Know Your ULB Submissions</small>
                <div class="card-val"><?php echo number_format($count_profiles); ?></div>
                <small class="opacity-80">Total Profile Surveys</small>
              </div>
            </div>
            <div class="col-md-3">
              <div class="admin-card card-amber">
                <small class="text-uppercase font-weight-bold">Daily Logs Today</small>
                <div class="card-val"><?php echo number_format($count_logs_today); ?></div>
                <small class="opacity-80">Submitted Today</small>
              </div>
            </div>
          </div>

          <!-- Admin Action Control Modules Grid -->
          <h5 class="font-weight-bold text-dark mb-3"><i class="typcn typcn-key"></i> System Control Modules</h5>
          <div class="row">

            <!-- Dynamic Question Builder Widget -->
            <div class="col-md-4">
              <div class="widget-box text-center h-100 d-flex flex-column justify-content-between">
                <div>
                  <div class="widget-icon"><i class="typcn typcn-document-add"></i></div>
                  <div class="widget-title">Dynamic Question Builder</div>
                  <div class="widget-desc">Add, edit, reorder, or delete questions for the "Know Your ULB" profile form dynamically without changing code.</div>
                </div>
                <a href="manage_questions.php" class="btn btn-primary btn-block font-weight-bold"><i class="typcn typcn-edit"></i> Manage Questions</a>
              </div>
            </div>

            <!-- User Account Manager Widget -->
            <div class="col-md-4">
              <div class="widget-box text-center h-100 d-flex flex-column justify-content-between">
                <div>
                  <div class="widget-icon"><i class="typcn typcn-group"></i></div>
                  <div class="widget-title">User Account Manager</div>
                  <div class="widget-desc">Create new ULB staff logins, reset/change passwords, edit user profiles, or revoke system access permissions.</div>
                </div>
                <a href="manage_users.php" class="btn btn-info btn-block font-weight-bold"><i class="typcn typcn-user-add"></i> Manage User Logins</a>
              </div>
            </div>

            <!-- ULB & Target Manager Widget -->
            <div class="col-md-4">
              <div class="widget-box text-center h-100 d-flex flex-column justify-content-between">
                <div>
                  <div class="widget-icon"><i class="typcn typcn-location-armchair"></i></div>
                  <div class="widget-title">ULB & Target Manager</div>
                  <div class="widget-desc">Add new ULB Towns, assign districts, and update total allocated street light point targets across Punjab.</div>
                </div>
                <a href="manage_ulbs.php" class="btn btn-success btn-block font-weight-bold"><i class="typcn typcn-edit"></i> Manage ULBs & Targets</a>
              </div>
            </div>

            <!-- Streetlight Data Audit Widget -->
            <div class="col-md-4 mt-4">
              <div class="widget-box text-center h-100 d-flex flex-column justify-content-between">
                <div>
                  <div class="widget-icon"><i class="typcn typcn-flash"></i></div>
                  <div class="widget-title">Streetlight Data Audit</div>
                  <div class="widget-desc">Audit, filter, edit, or fix daily streetlight operational logs and non-functional counts submitted by ULBs.</div>
                </div>
                <a href="manage_streetlight_data.php" class="btn btn-warning text-white btn-block font-weight-bold"><i class="typcn typcn-eye"></i> Audit Operational Logs</a>
              </div>
            </div>

            <!-- Know Your ULB MIS Reports Widget -->
            <div class="col-md-4 mt-4">
              <div class="widget-box text-center h-100 d-flex flex-column justify-content-between">
                <div>
                  <div class="widget-icon"><i class="typcn typcn-chart-bar-outline"></i></div>
                  <div class="widget-title">Know Your ULB Report & Excel</div>
                  <div class="widget-desc">View all submitted ULB profile survey records with 110+ columns and export directly to Excel.</div>
                </div>
                <a href="user_profile_report.php" class="btn btn-secondary btn-block font-weight-bold"><i class="typcn typcn-export"></i> View Reports & Export</a>
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
