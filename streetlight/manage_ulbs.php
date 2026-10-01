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

// Handle Delete ULB Action
if (isset($_GET['del_id']) && (int)$_GET['del_id'] > 0) {
  $del_id = (int)$_GET['del_id'];
  if ($con->query("DELETE FROM towns_streetlight_mapping WHERE town_id = $del_id")) {
    $msg = "ULB Town deleted successfully!";
  } else {
    $error = "Error deleting ULB town: " . $con->error;
  }
}

// Handle Add / Edit ULB Submission
if (isset($_POST['save_ulb'])) {
  $town_id = !empty($_POST['town_id']) ? (int)$_POST['town_id'] : 0;
  $town_name = mysqli_real_escape_string($con, $_POST['town_name']);
  $district_name = mysqli_real_escape_string($con, $_POST['district_name']);
  $distid = !empty($_POST['distid']) ? (int)$_POST['distid'] : 1;
  $target_value = !empty($_POST['target_value']) ? (int)$_POST['target_value'] : 0;

  if (empty($town_name) || empty($district_name)) {
    $error = "Please enter Town Name and District Name.";
  } else {
    if ($town_id > 0) {
      // Update
      $sql_t = "UPDATE towns_streetlight_mapping SET 
        town_name = '$town_name',
        district_name = '$district_name',
        distid = $distid,
        target_value = $target_value
        WHERE town_id = $town_id";
      if ($con->query($sql_t)) {
        $msg = "ULB Town details & target updated successfully!";
      } else {
        $error = "Error updating ULB town: " . $con->error;
      }
    } else {
      // Generate Next town_id
      $res_max = $con->query("SELECT MAX(town_id) as max_id FROM towns_streetlight_mapping");
      $new_town_id = 1;
      if ($res_max && $rm = $res_max->fetch_assoc()) {
        $new_town_id = $rm['max_id'] + 1;
      }

      $sql_t = "INSERT INTO towns_streetlight_mapping (town_id, town_name, district_name, distid, target_value) 
        VALUES ($new_town_id, '$town_name', '$district_name', $distid, $target_value)";
      if ($con->query($sql_t)) {
        $msg = "New ULB Town added successfully!";
      } else {
        $error = "Error adding ULB town: " . $con->error;
      }
    }
  }
}

// Load ULB for Editing if Edit ID passed
$edit_ulb = array();
if (isset($_GET['edit_id']) && (int)$_GET['edit_id'] > 0) {
  $res_et = $con->query("SELECT * FROM towns_streetlight_mapping WHERE town_id = " . (int)$_GET['edit_id']);
  if ($res_et && $res_et->num_rows > 0) {
    $edit_ulb = $res_et->fetch_assoc();
  }
}

// Fetch ULB List
$sql_ulbs = "SELECT * FROM towns_streetlight_mapping ORDER BY district_name, town_name";
$res_ulbs = $con->query($sql_ulbs);
?>
<!DOCTYPE html>
<html lang="en">

<head>
  <title>ULB & Target Manager || PMIDC Admin</title>
  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <style>
    .card-ulb {
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
    }
    .table-ulbs th { background-color: #f8fafc; font-weight: 700; color: #1e293b; }
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
              <p class="mb-0">ULB & Target Manager</p>
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

          <!-- Add / Edit ULB Form Card -->
          <div class="row">
            <div class="col-12 grid-margin stretch-card">
              <div class="card card-ulb">
                <div class="card-body">
                  <h4 class="card-title text-primary"><i class="typcn typcn-location-armchair"></i> <?php echo !empty($edit_ulb) ? "Edit ULB Town & Target" : "Add New ULB Town"; ?></h4>
                  <p class="card-description">
                    Manage Urban Local Bodies (ULBs) and set target allocated street light points.
                  </p>

                  <form method="post" action="manage_ulbs.php">
                    <input type="hidden" name="town_id" value="<?php echo isset($edit_ulb['town_id']) ? $edit_ulb['town_id'] : ''; ?>">

                    <div class="row">
                      <div class="col-md-4 form-group">
                        <label class="font-weight-bold">District Name <span class="text-danger">*</span></label>
                        <input type="text" name="district_name" class="form-control" placeholder="e.g. Amritsar" value="<?php echo isset($edit_ulb['district_name']) ? htmlspecialchars($edit_ulb['district_name']) : ''; ?>" required>
                      </div>

                      <div class="col-md-4 form-group">
                        <label class="font-weight-bold">ULB / Town Name <span class="text-danger">*</span></label>
                        <input type="text" name="town_name" class="form-control" placeholder="e.g. MC Amritsar" value="<?php echo isset($edit_ulb['town_name']) ? htmlspecialchars($edit_ulb['town_name']) : ''; ?>" required>
                      </div>

                      <div class="col-md-2 form-group">
                        <label class="font-weight-bold">District Code (ID)</label>
                        <input type="number" name="distid" class="form-control" placeholder="1" value="<?php echo isset($edit_ulb['distid']) ? $edit_ulb['distid'] : '1'; ?>">
                      </div>

                      <div class="col-md-2 form-group">
                        <label class="font-weight-bold">Target Streetlights <span class="text-danger">*</span></label>
                        <input type="number" name="target_value" class="form-control" placeholder="Total allocated count" value="<?php echo isset($edit_ulb['target_value']) ? $edit_ulb['target_value'] : '0'; ?>" required>
                      </div>
                    </div>

                    <div class="d-flex justify-content-between align-items-center mt-3">
                      <?php if (!empty($edit_ulb)): ?>
                        <a href="manage_ulbs.php" class="btn btn-outline-secondary btn-sm"><i class="typcn typcn-times"></i> Cancel Edit</a>
                      <?php else: ?>
                        <span></span>
                      <?php endif; ?>

                      <button type="submit" name="save_ulb" class="btn btn-primary font-weight-bold px-4">
                        <i class="typcn typcn-device-floppy"></i> <?php echo !empty($edit_ulb) ? "Update ULB Target" : "Add New ULB"; ?>
                      </button>
                    </div>

                  </form>
                </div>
              </div>
            </div>
          </div>

          <!-- Existing ULBs List Table -->
          <div class="row">
            <div class="col-12 grid-margin stretch-card">
              <div class="card card-ulb">
                <div class="card-body">
                  <h4 class="card-title text-dark mb-3"><i class="typcn typcn-th-list"></i> Registered ULB Towns & Target Allocations</h4>

                  <div class="table-responsive">
                    <table class="table table-bordered table-striped table-hover table-ulbs">
                      <thead>
                        <tr>
                          <th>Town ID</th>
                          <th>District Name</th>
                          <th>ULB / Town Name</th>
                          <th>Target Street Light Points</th>
                          <th>Action</th>
                        </tr>
                      </thead>
                      <tbody>
                        <?php
                        if ($res_ulbs && $res_ulbs->num_rows > 0) {
                          while ($t_row = $res_ulbs->fetch_assoc()) {
                        ?>
                            <tr>
                              <td>#<?php echo $t_row['town_id']; ?></td>
                              <td><strong><?php echo htmlspecialchars($t_row['district_name']); ?></strong></td>
                              <td><span class="text-primary font-weight-bold"><?php echo htmlspecialchars($t_row['town_name']); ?></span></td>
                              <td><span class="badge badge-success font-weight-bold" style="font-size:0.9rem;"><?php echo number_format((int)$t_row['target_value']); ?> Points</span></td>
                              <td>
                                <a href="manage_ulbs.php?edit_id=<?php echo $t_row['town_id']; ?>" class="btn btn-warning btn-xs text-white"><i class="typcn typcn-edit"></i> Edit Target</a>
                                <a href="manage_ulbs.php?del_id=<?php echo $t_row['town_id']; ?>" onclick="return confirm('Are you sure you want to delete this ULB town?')" class="btn btn-danger btn-xs"><i class="typcn typcn-trash"></i> Delete</a>
                              </td>
                            </tr>
                        <?php
                          }
                        } else {
                          echo "<tr><td colspan='5' class='text-center py-4 text-muted'>No ULB towns registered yet.</td></tr>";
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
