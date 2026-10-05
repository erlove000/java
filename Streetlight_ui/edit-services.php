<?php session_start();
error_reporting(0);
include_once('includes/config.php');
if (strlen($_SESSION['aid']==0)) {
  header('location:logout.php');
  } else{
if(isset($_POST['submit']))
{
$namesc=$_POST['namesc'];
$dob=$_POST['dob'];
$contnum=$_POST['contnum'];
$commadd=$_POST['commadd'];
$emeradd=$_POST['emeradd'];
$emercontnum=$_POST['emercontnum'];
$id=intval($_GET['id']);
$sql=mysqli_query($con,"update tblseniorcitizen set Name='$namesc',DateofBirth='$dob', ContactNumber='$contnum',CommunicationAddress='$commadd',EmergencyAddress='$emeradd',EmergencyContactnumber='$emercontnum' where ID='$id'");
echo "<script>alert('Senior citizen detail has been updated successfully');</script>";
echo "<script>window.location.href='manage-scdetails.php'</script>";

}
?>
<!DOCTYPE html>
<html lang="en">

<head>
  
  <title>SBMU || Particular release</title>  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
  <!-- base:css -->
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <!-- endinject -->
  <!-- plugin css for this page -->
  <link rel="stylesheet" href="vendors/select2/select2.min.css">
  <link rel="stylesheet" href="vendors/select2-bootstrap-theme/select2-bootstrap.min.css">
  <!-- End plugin css for this page -->
  <!-- inject:css -->
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <!-- endinject -->
  
  <script src="http://js.nicedit.com/nicEdit-latest.js" type="text/javascript"></script>
<script type="text/javascript">bkLib.onDomLoaded(nicEditors.allTextAreas);</script>
</head>

<body>
  <div class="container-scroller">
    <!-- partial:partials/_navbar.html -->

    <nav class="navbar col-lg-12 col-12 p-0 fixed-top d-flex flex-row">
      <div class="navbar-brand-wrapper d-flex justify-content-center">
        <div class="navbar-brand-inner-wrapper d-flex justify-content-between align-items-center w-100">
          <a class="navbar-brand brand-logo" href="index.html"><img src="images/logo.svg" alt="logo"/></a>
          <a class="navbar-brand brand-logo-mini" href="index.html"><img src="images/logo-mini.svg" alt="logo"/></a>
          <button class="navbar-toggler navbar-toggler align-self-center" type="button" data-toggle="minimize">
            <span class="typcn typcn-th-menu"></span>
          </button>
        </div>
      </div>
      <?php include_once('includes/header.php');?>
    </nav>
    <!-- partial -->
    <div class="container-fluid page-body-wrapper">
      <!-- partial:partials/_settings-panel.html -->
     
    <?php include_once('includes/sidebar.php');?>
      <!-- partial -->
      <!-- partial:partials/_sidebar.html -->
  
      <!-- partial -->
      <div class="main-panel">        
        <div class="content-wrapper">
          <div class="row">
            <div class="col-md-12 grid-margin stretch-card">
              <div class="card">
                <div class="card-body">
                  <h4 class="card-title">Viewing Release of release Id:- <?php $id=intval($_GET['id']); echo $id; ?> </h4>
              
                  <form class="forms-sample" method="post">
                    <?php
                    
                         $query=mysqli_query($con,"select s.*,d.district_name,t.town_name  from sbmu_release_data s, district d, sbmu_towns t where s.release_id=".$id."
                         and d.district_id=s.release_dist
                         and t.town_id=s.release_town");

while($row=mysqli_fetch_array($query))
{
?>
                    <div class="form-group">
                    <label for="exampleInputUsername1">Name of Scheme</label>
                      <input id="namesc" name="namesc" type="text" class="form-control" required="true" value="<?php echo htmlentities($row['release_scheme']);?> " readonly>
                    </div>  
                    <div class="form-group">
                       <label for="exampleInputUsername1">Name of District</label>
                      <input id="namesc" name="namesc" type="text" class="form-control" required="true" value="<?php echo htmlentities($row['district_name']);?>" readonly>
                    </div>
                          <div class="form-group">
                       <label for="exampleInputUsername1">Name of town</label>
                      <input id="namesc" name="namesc" type="text" class="form-control" required="true" value="<?php echo htmlentities($row['town_name']);?>" readonly>
                    </div>
                 
                    <div class="form-group">
                       <label for="exampleInputUsername1">Released Category</label>
                      <input id="namesc" name="namesc" type="text" class="form-control" required="true" value="<?php echo htmlentities($row['release_cat']);?>" readonly>
                    </div>
                    <div class="form-group">
                       <label for="exampleInputUsername1">Released Seats</label>
                      <input id="namesc" name="namesc" type="text" class="form-control" required="true" value="<?php echo htmlentities($row['release_seats']);?>" readonly>
                    </div>
                    <div class="form-group">
                       <label for="exampleInputUsername1">Released Amount</label>
                      <input id="namesc" name="namesc" type="text" class="form-control" required="true" value="<?php echo htmlentities($row['released_amount']);?>" readonly>
                    </div>
                    <div class="form-group">
                       <label for="exampleInputUsername1">Date of Release </label>
                      <input id="namesc" name="namesc" type="text" class="form-control" required="true" value="<?php echo htmlentities($row['release_date']);?>" readonly>
                    </div>
                    
                    <?php } ?>
                    <!--<button type="submit" class="btn btn-primary mr-2" name="submit">Submit</button>-->
                  </form>
                </div>
              </div>
            </div>
     
          </div>
        </div>
        <!-- content-wrapper ends -->
       <?php include_once('includes/footer.php');?>
      </div>
      <!-- main-panel ends -->
    </div>
    <!-- page-body-wrapper ends -->
  </div>
  <!-- container-scroller -->
  <!-- base:js -->
  <script src="vendors/js/vendor.bundle.base.js"></script>
  <!-- endinject -->
  <!-- inject:js -->
  <script src="js/off-canvas.js"></script>
  <script src="js/hoverable-collapse.js"></script>
  <script src="js/template.js"></script>
  <script src="js/settings.js"></script>
  <script src="js/todolist.js"></script>
  <!-- endinject -->
  <!-- plugin js for this page -->
  <script src="vendors/typeahead.js/typeahead.bundle.min.js"></script>
  <script src="vendors/select2/select2.min.js"></script>
  <!-- End plugin js for this page -->
  <!-- Custom js for this page-->
  <script src="js/file-upload.js"></script>
  <script src="js/typeahead.js"></script>
  <script src="js/select2.js"></script>
  <!-- End custom js for this page-->
</body>

</html>
<?php } ?>