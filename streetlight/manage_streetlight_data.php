<?php
session_start();
error_reporting(0);
include('includes/config.php');

if (strlen($_SESSION['aid']) == 0) {
  header('location:index.php');
  exit();
}

// Strict Admin Authorization Check (townid == 0)
$res_auth = $con->query("SELECT townid FROM streetlightlogin WHERE id=" . (int)$_SESSION['aid']);
if ($res_auth && $r_a = $res_auth->fetch_assoc()) {
  if ($r_a['townid'] != 0) {
    header('location:dashboard.php');
    exit();
  }
}

$msg = "";
$error = "";

// Handle Delete Log Entry Action
if (isset($_GET['del_id']) && (int)$_GET['del_id'] > 0) {
  $del_id = (int)$_GET['del_id'];
  if ($con->query("DELETE FROM streetlightdata WHERE id = $del_id")) {
    $msg = "Streetlight operational log entry deleted successfully!";
  } else {
    $error = "Error deleting operational log: " . $con->error;
  }
}

// Handle Update Log Submission
if (isset($_POST['update_log'])) {
  $log_id = (int)$_POST['log_id'];
  $total_working = (int)$_POST['total_working_street_light'];
  $total_non_working = (int)$_POST['total_non_working_street_light'];
  $last_24h = (int)$_POST['total_not_working_last_24hrs'];
  $not_working_today = (int)$_POST['total_not_working_today'];
  $nonfunctional = (int)$_POST['nonfunctionaltoday'];
  $remarks = mysqli_real_escape_string($con, $_POST['remarks']);

  $sql_u = "UPDATE streetlightdata SET 
    total_working_street_light = $total_working,
    total_non_working_street_light = $total_non_working,
    total_not_working_last_24hrs = $last_24h,
    total_not_working_today = $not_working_today,
    nonfunctionaltoday = $nonfunctional,
    remarks = '$remarks'
    WHERE id = $log_id";

  if ($con->query($sql_u)) {
    $msg = "Operational log record updated successfully!";
  } else {
    $error = "Error updating log record: " . $con->error;
  }
}

// Filter handling
$where_clauses = array();
if (!empty($_GET['dist_id'])) {
  $where_clauses[] = "tt.distid = " . (int)$_GET['dist_id'];
}
if (!empty($_GET['town_id'])) {
  $where_clauses[] = "sd.town_id = " . (int)$_GET['town_id'];
}

$where_sql = "";
if (count($where_clauses) > 0) {
  $where_sql = " WHERE " . implode(" AND ", $where_clauses);
}

// Load Log for Editing
$edit_log = array();
if (isset($_GET['edit_id']) && (int)$_GET['edit_id'] > 0) {
  $res_el = $con->query("SELECT sd.*, tt.town_name, tt.district_name FROM streetlightdata sd INNER JOIN towns_streetlight_mapping tt ON sd.town_id = tt.town_id WHERE sd.id = " . (int)$_GET['edit_id']);
  if ($res_el && $res_el->num_rows > 0) {
    $edit_log = $res_el->fetch_assoc();
  }
}

// Fetch Log List
$sql_logs = "SELECT sd.*, tt.town_name, tt.district_name 
  FROM streetlightdata sd 
  INNER JOIN towns_streetlight_mapping tt ON sd.town_id = tt.town_id 
  $where_sql 
  ORDER BY sd.DOA DESC LIMIT 100";
$res_logs = $con->query($sql_logs);
?>
<!DOCTYPE html>
<html lang="en">

<head>
  <title>Streetlight Data Audit & Control || PMIDC Admin</title>
  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <style>
    .card-audit {
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
    }
    .table-logs th { background-color: #f8fafc; font-weight: 700; color: #1e293b; }
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
              <p class="mb-0">Streetlight Data Audit</p>
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

          <?php if (!empty($edit_log)): ?>
            <!-- Edit Log Form -->
            <div class="row">
              <div class="col-12 grid-margin stretch-card">
                <div class="card card-audit">
                  <div class="card-body">
                    <h4 class="card-title text-warning"><i class="typcn typcn-edit"></i> Edit Log Entry for <?php echo htmlspecialchars($edit_log['town_name']); ?> (Date: <?php echo date('d-M-Y', strtotime($edit_log['DOA'])); ?>)</h4>

                    <form method="post" action="manage_streetlight_data.php">
                      <input type="hidden" name="log_id" value="<?php echo $edit_log['id']; ?>">

                      <div class="row">
                        <div class="col-md-3 form-group">
                          <label class="font-weight-bold">Total Working Lights</label>
                          <input type="number" name="total_working_street_light" class="form-control" value="<?php echo $edit_log['total_working_street_light']; ?>" required>
                        </div>
                        <div class="col-md-3 form-group">
                          <label class="font-weight-bold">Total Non-Working Lights</label>
                          <input type="number" name="total_non_working_street_light" class="form-control" value="<?php echo $edit_log['total_non_working_street_light']; ?>" required>
                        </div>
                        <div class="col-md-3 form-group">
                          <label class="font-weight-bold">Not Working (Last 24h)</label>
                          <input type="number" name="total_not_working_last_24hrs" class="form-control" value="<?php echo $edit_log['total_not_working_last_24hrs']; ?>">
                        </div>
                        <div class="col-md-3 form-group">
                          <label class="font-weight-bold">Total Non-Functional</label>
                          <input type="number" name="nonfunctionaltoday" class="form-control" value="<?php echo $edit_log['nonfunctionaltoday']; ?>">
                        </div>
                        <div class="col-md-12 form-group">
                          <label class="font-weight-bold">Remarks</label>
                          <input type="text" name="remarks" class="form-control" value="<?php echo htmlspecialchars($edit_log['remarks']); ?>">
                        </div>
                      </div>

                      <div class="d-flex justify-content-between align-items-center mt-2">
                        <a href="manage_streetlight_data.php" class="btn btn-outline-secondary btn-sm"><i class="typcn typcn-times"></i> Cancel Edit</a>
                        <button type="submit" name="update_log" class="btn btn-warning text-white font-weight-bold px-4"><i class="typcn typcn-device-floppy"></i> Update Log Record</button>
                      </div>
                    </form>
                  </div>
                </div>
              </div>
            </div>
          <?php endif; ?>

          <!-- Logs Registry & Filter Table -->
          <div class="row">
            <div class="col-12 grid-margin stretch-card">
              <div class="card card-audit">
                <div class="card-body">
                  <div class="d-flex justify-content-between align-items-center mb-3">
                    <h4 class="card-title text-dark mb-0"><i class="typcn typcn-flash"></i> Streetlight Operational Logs Registry</h4>

                    <form method="get" class="form-inline">
                      <select name="dist_id" class="form-control form-control-sm mx-1" onchange="this.form.submit()">
                        <option value="">-- All Districts --</option>
                        <?php
                        $res_d = $con->query("SELECT DISTINCT distid, district_name FROM towns_streetlight_mapping ORDER BY district_name");
                        while ($rd = $res_d->fetch_assoc()) {
                          $sel = ($_GET['dist_id'] == $rd['distid']) ? 'selected' : '';
                          echo "<option value='" . $rd['distid'] . "' $sel>" . htmlspecialchars($rd['district_name']) . "</option>";
                        }
                        ?>
                      </select>
                      <select name="town_id" class="form-control form-control-sm mx-1" onchange="this.form.submit()">
                        <option value="">-- All ULBs --</option>
                        <?php
                        $res_t = $con->query("SELECT town_id, town_name FROM towns_streetlight_mapping ORDER BY town_name");
                        while ($rt = $res_t->fetch_assoc()) {
                          $sel = ($_GET['town_id'] == $rt['town_id']) ? 'selected' : '';
                          echo "<option value='" . $rt['town_id'] . "' $sel>" . htmlspecialchars($rt['town_name']) . "</option>";
                        }
                        ?>
                      </select>
                    </form>
                  </div>

                  <div class="table-responsive">
                    <table class="table table-bordered table-striped table-hover table-logs">
                      <thead>
                        <tr>
                          <th>ID</th>
                          <th>District</th>
                          <th>ULB Name</th>
                          <th>Date (DOA)</th>
                          <th>Working Lights</th>
                          <th>Non-Working</th>
                          <th>Last 24h Defective</th>
                          <th>Remarks</th>
                          <th>Added By</th>
                          <th>Action</th>
                        </tr>
                      </thead>
                      <tbody>
                        <?php
                        if ($res_logs && $res_logs->num_rows > 0) {
                          while ($l_row = $res_logs->fetch_assoc()) {
                        ?>
                            <tr>
                              <td>#<?php echo $l_row['id']; ?></td>
                              <td><strong><?php echo htmlspecialchars($l_row['district_name']); ?></strong></td>
                              <td><?php echo htmlspecialchars($l_row['town_name']); ?></td>
                              <td><span class="badge badge-light"><?php echo date('d-M-Y', strtotime($l_row['DOA'])); ?></span></td>
                              <td><span class="badge badge-success font-weight-bold"><?php echo number_format((int)$l_row['total_working_street_light']); ?></span></td>
                              <td><span class="badge badge-danger font-weight-bold"><?php echo number_format((int)$l_row['total_non_working_street_light']); ?></span></td>
                              <td><?php echo (int)$l_row['total_not_working_last_24hrs']; ?></td>
                              <td><small><?php echo htmlspecialchars($l_row['remarks']); ?></small></td>
                              <td><small><?php echo htmlspecialchars($l_row['Addedby']); ?></small></td>
                              <td>
                                <a href="manage_streetlight_data.php?edit_id=<?php echo $l_row['id']; ?>" class="btn btn-warning btn-xs text-white"><i class="typcn typcn-edit"></i> Edit</a>
                                <a href="manage_streetlight_data.php?del_id=<?php echo $l_row['id']; ?>" onclick="return confirm('Are you sure you want to delete this log entry?')" class="btn btn-danger btn-xs"><i class="typcn typcn-trash"></i> Delete</a>
                              </td>
                            </tr>
                        <?php
                          }
                        } else {
                          echo "<tr><td colspan='10' class='text-center py-4 text-muted'>No operational logs found.</td></tr>";
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
