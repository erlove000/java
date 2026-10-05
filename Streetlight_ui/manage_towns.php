<?php
session_start();
error_reporting(0);
include('includes/config.php');
if (strlen($_SESSION['aid']) == 0) {
    header('location:index.php');
    exit();
}

// Ensure Admin Access (Town 0)
$aid = (int)$_SESSION['aid'];
$admin_check = mysqli_query($con, "SELECT townid FROM streetlightlogin WHERE id=$aid");
$admin_row = mysqli_fetch_assoc($admin_check);
if ($admin_row['townid'] != 0) {
    echo "Access Denied. Admins only.";
    exit();
}

$filter_distid = isset($_GET['distid']) ? (int)$_GET['distid'] : '';

// Add Town
if(isset($_POST['add_town'])) {
    $distid = (int)$_POST['distid'];
    // get district name
    $d_q = mysqli_query($con, "SELECT district_name FROM towns_streetlight_mapping WHERE distid=$distid LIMIT 1");
    $d_row = mysqli_fetch_assoc($d_q);
    $district_name = mysqli_real_escape_string($con, $d_row['district_name']);
    
    $town_id = (int)$_POST['town_id'];
    $town_name = mysqli_real_escape_string($con, $_POST['town_name']);
    
    // Check if town_id already exists
    $chk = mysqli_query($con, "SELECT town_id FROM towns_streetlight_mapping WHERE town_id=$town_id");
    if(mysqli_num_rows($chk) > 0) {
        $msg = "<div class='alert alert-warning'>Town ID $town_id already exists!</div>";
    } else {
        $insert = mysqli_query($con, "INSERT INTO towns_streetlight_mapping (distid, district_name, town_id, town_name) VALUES ($distid, '$district_name', $town_id, '$town_name')");
        if($insert) { $msg = "<div class='alert alert-success'>Town added successfully!</div>"; }
        else { $msg = "<div class='alert alert-danger'>Failed to add town.</div>"; }
    }
}

// Edit Town
if(isset($_POST['edit_town'])) {
    $old_town_id = (int)$_POST['old_town_id'];
    $town_id = (int)$_POST['town_id'];
    $town_name = mysqli_real_escape_string($con, $_POST['town_name']);
    
    // Note: if town_id changed, check if new town_id exists
    $proceed = true;
    if($old_town_id != $town_id) {
        $chk = mysqli_query($con, "SELECT town_id FROM towns_streetlight_mapping WHERE town_id=$town_id");
        if(mysqli_num_rows($chk) > 0) {
            $msg = "<div class='alert alert-warning'>New Town ID $town_id already exists!</div>";
            $proceed = false;
        }
    }
    
    if($proceed) {
        $update = mysqli_query($con, "UPDATE towns_streetlight_mapping SET town_id=$town_id, town_name='$town_name' WHERE town_id=$old_town_id");
        if($update) { 
            // Update other tables if town_id changed
            if($old_town_id != $town_id) {
                mysqli_query($con, "UPDATE streetlightlogin SET townid=$town_id WHERE townid=$old_town_id");
                mysqli_query($con, "UPDATE ulb_user_profile SET town_id=$town_id WHERE town_id=$old_town_id");
                mysqli_query($con, "UPDATE streetlightdata SET town_id=$town_id WHERE town_id=$old_town_id");
                mysqli_query($con, "UPDATE survey_answers SET town_id=$town_id WHERE town_id=$old_town_id");
            }
            $msg = "<div class='alert alert-success'>Town updated successfully!</div>"; 
        }
        else { $msg = "<div class='alert alert-danger'>Failed to update town.</div>"; }
    }
}

?>
<!DOCTYPE html>
<html lang="en">
<head>
    <title>ULB / Town Master || PMIDC</title>
    <link rel="stylesheet" href="vendors/typicons/typicons.css">
    <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
    <link rel="stylesheet" href="css/vertical-layout-light/style.css">
</head>
<body>
<div class="container-scroller">
    <?php include_once('includes/header.php'); ?>
    <div class="container-fluid page-body-wrapper">
        <?php include_once('includes/sidebar.php'); ?>
        <div class="main-panel">
            <div class="content-wrapper">
                <div class="row">
                    <div class="col-12 grid-margin stretch-card">
                        <div class="card shadow-sm border-0" style="border-radius: 8px;">
                            <div class="card-body">
                                <h4 class="card-title text-primary"><i class="typcn typcn-location-outline"></i> ULB / Town Master List</h4>
                                <p class="card-description">View all Town IDs and District IDs across the state.</p>
                                
                                <form method="GET" action="manage_towns.php" class="form-inline mb-4">
                                    <label class="mr-sm-2 font-weight-bold" for="distid">Filter by District: </label>
                                    <select class="form-control mb-2 mr-sm-2 p-2" name="distid" id="distid" onchange="this.form.submit()">
                                        <option value="">-- All Districts --</option>
                                        <?php
                                        // Fetch distinct districts
                                        $dist_res = mysqli_query($con, "SELECT DISTINCT distid, district_name FROM towns_streetlight_mapping ORDER BY district_name ASC");
                                        while($d = mysqli_fetch_assoc($dist_res)) {
                                            $sel = ($filter_distid == $d['distid']) ? 'selected' : '';
                                            echo "<option value='".$d['distid']."' $sel>".htmlspecialchars($d['district_name'])." (ID: ".$d['distid'].")</option>";
                                        }
                                        ?>
                                    </select>
                                    <a href="manage_towns.php" class="btn btn-outline-secondary mb-2 ml-2">Reset Filter</a>
                                    <button type="button" class="btn btn-primary mb-2" style="margin-left: auto;" data-bs-toggle="modal" data-bs-target="#addTownModal">
                                        <i class="typcn typcn-plus"></i> Add Town
                                    </button>
                                </form>
                                <?php if(isset($msg)) echo $msg; ?>

                                <div class="table-responsive">
                                    <table class="table table-hover table-bordered table-striped">
                                        <thead class="bg-light">
                                            <tr>
                                                <th>#</th>
                                                <th>District ID</th>
                                                <th>District Name</th>
                                                <th>Town ID</th>
                                                <th>Town / ULB Name</th>
                                                <th>Action</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            <?php
                                            $where = "";
                                            if ($filter_distid > 0) {
                                                $where = " WHERE distid = $filter_distid ";
                                            }
                                            $towns_q = mysqli_query($con, "SELECT distid, district_name, town_id, town_name FROM towns_streetlight_mapping $where ORDER BY district_name ASC, town_name ASC");
                                            
                                            $cnt = 1;
                                            if(mysqli_num_rows($towns_q) > 0) {
                                                while($t = mysqli_fetch_assoc($towns_q)) {
                                                    echo "<tr>";
                                                    echo "<td>$cnt</td>";
                                                    echo "<td><span class='badge badge-secondary'>".$t['distid']."</span></td>";
                                                    echo "<td class='font-weight-bold'>".htmlspecialchars($t['district_name'])."</td>";
                                                    echo "<td><span class='badge badge-primary'>".$t['town_id']."</span></td>";
                                                    echo "<td>".htmlspecialchars($t['town_name'])."</td>";
                                                    echo "<td>
                                                            <button class='btn btn-sm btn-primary' data-bs-toggle='modal' data-bs-target='#editTownModal".$t['town_id']."'><i class='typcn typcn-edit'></i> Edit</button>
                                                          </td>";
                                                    echo "</tr>";
                                                    ?>
                                                    <!-- Edit Town Modal -->
                                                    <div class="modal fade" id="editTownModal<?php echo $t['town_id']; ?>" tabindex="-1" aria-hidden="true">
                                                      <div class="modal-dialog">
                                                        <div class="modal-content" style="border-radius:12px; border:none;">
                                                          <div class="modal-header" style="background:#f8fafc; border-bottom:1px solid #e2e8f0;">
                                                            <h5 class="modal-title font-weight-bold">Edit Town / ULB</h5>
                                                            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close">x</button>
                                                          </div>
                                                          <form method="post">
                                                              <div class="modal-body p-4">
                                                                  <input type="hidden" name="old_town_id" value="<?php echo $t['town_id']; ?>">
                                                                  <div class="mb-3">
                                                                      <label class="form-label font-weight-bold">District</label>
                                                                      <input type="text" class="form-control p-2" value="<?php echo htmlspecialchars($t['district_name']); ?>" readonly>
                                                                  </div>
                                                                  <div class="mb-3">
                                                                      <label class="form-label font-weight-bold">Town ID</label>
                                                                      <input type="number" name="town_id" class="form-control p-2" required value="<?php echo htmlspecialchars($t['town_id']); ?>">
                                                                  </div>
                                                                  <div class="mb-3">
                                                                      <label class="form-label font-weight-bold">Town / ULB Name</label>
                                                                      <input type="text" name="town_name" class="form-control p-2" required value="<?php echo htmlspecialchars($t['town_name']); ?>">
                                                                  </div>
                                                              </div>
                                                              <div class="modal-footer" style="border-top:1px solid #e2e8f0;">
                                                                  <button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancel</button>
                                                                  <button type="submit" name="edit_town" class="btn btn-primary">Save Changes</button>
                                                              </div>
                                                          </form>
                                                        </div>
                                                      </div>
                                                    </div>
                                                    <?php
                                                    $cnt++;
                                                }
                                            } else {
                                                echo "<tr><td colspan='5' class='text-center text-muted'>No towns found for this selection.</td></tr>";
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

<!-- Add Town Modal -->
<div class="modal fade" id="addTownModal" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog">
    <div class="modal-content" style="border-radius:12px; border:none;">
      <div class="modal-header" style="background:#f8fafc; border-bottom:1px solid #e2e8f0;">
        <h5 class="modal-title font-weight-bold">Add New Town / ULB</h5>
        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close">x</button>
      </div>
      <form method="post">
          <div class="modal-body p-4">
              <div class="mb-3">
                  <label class="form-label font-weight-bold">Select District</label>
                  <select name="distid" class="form-control p-2" required>
                      <option value="">-- Select District --</option>
                      <?php
                      $dist_res2 = mysqli_query($con, "SELECT DISTINCT distid, district_name FROM towns_streetlight_mapping ORDER BY district_name ASC");
                      while($d2 = mysqli_fetch_assoc($dist_res2)) {
                          echo "<option value='".$d2['distid']."'>".htmlspecialchars($d2['district_name'])." (ID: ".$d2['distid'].")</option>";
                      }
                      ?>
                  </select>
              </div>
              <div class="mb-3">
                  <label class="form-label font-weight-bold">Town ID</label>
                  <input type="number" name="town_id" class="form-control p-2" required placeholder="e.g. 101">
              </div>
              <div class="mb-3">
                  <label class="form-label font-weight-bold">Town / ULB Name</label>
                  <input type="text" name="town_name" class="form-control p-2" required placeholder="e.g. Amritsar(MC)">
              </div>
          </div>
          <div class="modal-footer" style="border-top:1px solid #e2e8f0;">
              <button type="button" class="btn btn-light" data-bs-dismiss="modal">Cancel</button>
              <button type="submit" name="add_town" class="btn btn-primary">Add Town</button>
          </div>
      </form>
    </div>
  </div>
</div>

<script src="vendors/js/vendor.bundle.base.js"></script>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
