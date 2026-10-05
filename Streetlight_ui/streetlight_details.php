<?php session_start();
error_reporting(0);
// ini_set('display_errors', 1);
// ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

include('includes/config.php');
if (strlen($_SESSION['aid']==0)) {
  header('location:logout.php');
  } else{
              
$sql2 = "SELECT districtid, townid,staff_name  FROM streetlightlogin where id=".$_SESSION['aid'];
$results = $con->query($sql2);
//echo $sql2;
if ($results->num_rows > 0) {
 
  while ($row = mysqli_fetch_assoc($results)) {
    $districtid = $row['districtid'];
    $townid=$row['townid']; 
    $loggername=$row['staff_name'];
  }
}  

$sql2 = "SELECT target_value as value  FROM towns_streetlight_mapping where town_id=".$townid;
$results = $con->query($sql2);
//echo $sql2;
if ($results->num_rows > 0) {
 
  while ($row = mysqli_fetch_assoc($results)) {
    $value = $row['value'];
  }
}  
 if ($value=='0'|| $value==0)
 {
  echo '<script>
  alert("Please Add the Total Street Light point, Contact PMIDC");
</script>';
 }
    if (isset($_POST['sa'])) {

      // Use isset() for compatibility with all PHP versions
      $distid = isset($_POST['district']) ? $_POST['district'] : null;
      $town_id = isset($_POST['town_id']) ? $_POST['town_id'] : null;
        $total_street_light_alloted = isset($_POST['target']) ? $_POST['target'] : null;
      $dob = isset($_POST['dob']) ? $_POST['dob'] : null;
      $working = isset($_POST['working']) ? $_POST['working'] : null;
      $non_working = isset($_POST['non_working']) ? $_POST['non_working'] : null;
      $town_name = isset($_POST['town']) ? $_POST['town'] : null;
      $under_repair = isset($_POST['under_repair']) ? $_POST['under_repair'] : null;
       $district = isset($_POST['district']) ? $_POST['district'] : null;
      $new_complaints = isset($_POST['new_complaints']) ? $_POST['new_complaints'] : null;
      $target = isset($_POST['target']) ? $_POST['target'] : null;
      $loggername = isset($_POST['loggername']) ? $_POST['loggername'] : null;
      $non_functional = isset($_POST['non_functional']) ? $_POST['non_functional'] : null;
      $remarks = isset($_POST['remarks']) ? $_POST['remarks'] : null;

      $query1 = "SELECT DATE(DOA) AS datedata FROM streetlightdata WHERE town_id = '$townid' and DOA ='".$dob." 00:00:00'";
      $results = $con->query($query1);
  #echo $query1;
 
  $istrue=false;
  if ($results && $results->num_rows > 0) {
    // Fetch and output the data if any rows are returned
    while ($row = $results->fetch_assoc()) {
      $istrue = true;
      echo '<script>
            alert("Data For Same Date is Already present.");
            window.location.href = "dashboard.php"; // Redirect after the alert is closed
        </script>';

    }
}  

    
        if ($istrue==false) {
        $stmt = $con->prepare("INSERT INTO streetlightdata 
        (distid, town_id,  total_street_light_alloted, total_working_street_light, total_non_working_street_light, total_not_working_last_24hrs, total_not_working_today, nonfunctionaltoday,remarks,Addedby,DOA) 
        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    // Check if the prepare() was successful
    if ($stmt === false) {
        die("Prepare failed: " . $con->error);
    }

    // Bind the parameters to the SQL query
    $stmt->bind_param("sssssssssss", $distid, $town_id,  $total_street_light_alloted, $working, $non_working, $under_repair, $new_complaints, $non_functional, $remarks,$loggername,$dob);

    // Execute the statement
    if ($stmt->execute()) {
      echo '<script>
      alert("Data inserted successfully!");
      window.location.href = "dashboard.php"; // Redirect after alert
  </script>';

    } else {
        echo '<script>alert("Error: ' . $stmt->error . '");</script>';
    }
  }
    // Close the statement
    $stmt->close();
      }
    

  
?>
<!DOCTYPE html>
<html lang="en">

<head>
  
  <title>Streetlight || Add  Details</title>  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
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
                  <h4 class="card-title">Add Details For Streetlight</h4>
                 

                  
                 

<form class="forms-sample"  method="POST"  id="myForm"  action="<?php echo $_SERVER['PHP_SELF']; ?>">  

<div class="form-group">
                       <label for="exampleInputUsername1">District Name (ਜ਼ਿਲ੍ਹੇ ਦਾ ਨਾਮ)</label>
                    
                       <?php

if ($townid == '0') {
                        
  $query="SELECT district_name, distid FROM towns_streetlight_mapping group by district_name, distid ";
} else {
  $query="SELECT district_name, distid FROM towns_streetlight_mapping where town_id = ".$townid;

}
$result = mysqli_query($con, $query);

// Check if there are any results
if (mysqli_num_rows($result) > 0) {
    //echo '<label for="town_id">Name of ULB</label>';
    echo '<select name="district" id="district" class="form-control" required="true" style="border: 1px solid #ccc; border-radius: 20px; height: 40px;">';
    echo "<option value=''>Select Town From Below</option>";
    // Loop through the results and generate select tag options
    while ($row = mysqli_fetch_assoc($result)) {
        $town = $row['district_name'];
    
        $distid = $row['distid'];
        echo "<option value='$distid'>$town</option>";
    }
    
    echo '</select>';
} 
?>

   
                    </div>
             

                   
                    <div class="form-group">
                  
                    <?php
if ($townid == '0') {
  $query = "SELECT town_name, town_id FROM towns_streetlight_mapping ";
} else {
  $query = "SELECT town_name, town_id FROM towns_streetlight_mapping WHERE town_id = " . $townid;
}

$result = mysqli_query($con, $query);

if (mysqli_num_rows($result) > 0) {
    echo '<label for="town_id">Name of ULB (ਯੂ.ਐਲ.ਬੀ. ਦਾ ਨਾਮ)</label>';
    echo '<select id="town_id" name="town_id" class="form-control" required="true" style="border: 1px solid #ccc; border-radius: 20px; height: 40px;" onchange="setTownId(this)">';
    echo "<option value=''>Select Town From Below</option>";

    // Loop through the results and generate select tag options
    while ($row = mysqli_fetch_assoc($result)) {
        $town = $row['town_name'];
        $town_id = $row['town_id'];
        echo "<option value='$town_id'>$town</option>";
    }

    echo '</select>';

    echo "<input id='loggername' name='loggername' type='hidden' class='form-control' value='$loggername' readonly style='border: 1px solid #ccc; border-radius: 20px; height: 40px;'>";
}
?>

<script>
function setTownId(selectElement) {
    // Get the selected town_id
    var selectedTownId = selectElement.value;
    
    // Set the value of the text box with the selected town_id
    document.getElementById('town_id').value = selectedTownId;
}
</script>
         
              </div>
  
                    <div class="form-group">
                 
    
                  <div class="form-group">
                      <label for="exampleInputEmail1">3.) Date of Reporting (ਰਿਪੋਰਟਿੰਗ ਦੀ ਮਿਤੀ)</label>
                     <input id="dob" name="dob" style=' border: 1px solid #ccc;  border-radius: 20px; height:40px' type="date" class="form-control" required="true" min="2020-01-01" max="<?php echo date('Y-m-d'); ?>">
                    </div>



                    <?php


$query = "SELECT * FROM towns_streetlight_mapping where town_id=".$townid; // Replace 'options' with your table name
$result = mysqli_query($con, $query);

// Check if there are any results
if (mysqli_num_rows($result) > 0) {
    echo '<label for="exampleInputEmail1">4.) Total No Of Street lights Points (ਕੁੱਲ ਸਟ੍ਰੀਟ ਲਾਈਟ ਪੁਆਇੰਟ)</label>';

    // Loop through the results and generate select tag options
    while ($row = mysqli_fetch_assoc($result)) {
        $target = $row['target_value'];
        echo "<input id='target' name='target' type='text' class='form-control' required='true' value='$target' readonly style='border: 1px solid #ccc; border-radius: 20px; height:40px'>";
    }
}
?>
<br>

<div class="form-group">
    <label for="exampleInputEmail1">5.) Total No Of Street lights Points Working(ਕੁੱਲ ਚੱਲ ਰਹੇ ਸਟ੍ਰੀਟ ਲਾਈਟ ਪੁਆਇੰਟ)</label>
    <input id="working" name="working" style='border: 1px solid #ccc; border-radius: 20px; height:40px' pattern="[0-9]+" maxlength="10" type="text" class="form-control" required="true" value="">
</div>

<div class="form-group">
    <label for="exampleInputEmail1">6.)  Total No Of Street lights Points Not Working ( ਕੁੱਲ ਨਾ-ਚੱਲ ਰਹੇ ਸਟ੍ਰੀਟ ਲਾਈਟ ਪੁਆਇੰਟ)</label>
    <input id="non_working" name="non_working" style='border: 1px solid #ccc; border-radius: 20px; height:40px' pattern="[0-9]+" maxlength="10" type="text" class="form-control" required="true" value="">
</div>
<div class="form-group">
    <label for="exampleInputEmail1">7.)  Total No Of Street lights Made Functional Today (ਅੱਜ ਚਾਲੂ ਕੀਤੀਆਂ ਗਈਆਂ ਕੁੱਲ ਸਟ੍ਰੀਟ ਲਾਈਟਾਂ)</label>
    <input id="under_repair" name="under_repair" style='border: 1px solid #ccc; border-radius: 20px; height:40px' pattern="[0-9]+" maxlength="10" type="text" class="form-control" required="true" value="">
</div>

<div class="form-group">
    <label for="exampleInputEmail1">8.)  Total No Of Points Not Working Today (New Complaints) (ਅੱਜ ਕੰਮ ਨਾ ਕਰ ਰਹੇ ਪੁਆਇੰਟ (ਨਵੀਆਂ ਸ਼ਿਕਾਇਤਾਂ)</label>
    <input id="new_complaints" name="new_complaints" style='border: 1px solid #ccc; border-radius: 20px; height:40px' pattern="[0-9]+" maxlength="10" type="text" class="form-control" required="true" value="">
</div>

<div class="form-group">
    <label for="exampleInputEmail1">9.)  Total Non Functional Street Points (ਨਾ-ਚੱਲ ਰਹੇ ਸਟ੍ਰੀਟ ਲਾਈਟ ਪੁਆਇੰਟ)</label>
    <input id="non_functional" name="non_functional" style='border: 1px solid #ccc; border-radius: 20px; height:40px' pattern="[0-9]+" maxlength="10" type="text" class="form-control" required="true" readonly value="">
</div>

<script>
function calculateNonFunctional() {
    var nonWorking = parseInt(document.getElementById('non_working').value) || 0;
    var underRepair = parseInt(document.getElementById('under_repair').value) || 0;
    var newComplaints = parseInt(document.getElementById('new_complaints').value) || 0;
    var working = parseInt(document.getElementById('working').value) || 0;
    var target = parseInt(document.getElementById('target').value) || 0;

    // Calculate the remaining total after repair
    var remaining = nonWorking - underRepair;

    // Calculate the final non-functional points
    var nonFunctional = remaining + newComplaints;

    // Check if the sum of working and non-functional points exceeds the target
    if (working + nonFunctional > target) {
        alert("The total of working and non-functional points cannot be greater than the total points of streetlight.\n ਚੱਲ ਰਹੇ ਅਤੇ ਨਾ-ਚੱਲ ਰਹੇ ਪੁਆਇੰਟਾਂ ਦਾ ਕੁੱਲ ਸਟ੍ਰੀਟ ਲਾਈਟ ਪੁਆਇੰਟਾਂ ਤੋਂ ਵੱਧ ਨਹੀਂ ਹੋ ਸਕਦਾ।");
        document.getElementById('working').value = '';
        document.getElementById('non_functional').value = '';
        document.getElementById('non_working').value = '';
        document.getElementById('under_repair').value = '';
        document.getElementById('new_complaints').value = ''; 
        return; // Exit the function if the condition is met
    }

    if ( nonFunctional < 0) {
        alert("Total Non Functional Street Points cannot be in less than 0.\n ਕੁੱਲ ਨਾ-ਚੱਲ ਰਹੇ ਸਟ੍ਰੀਟ ਪੁਆਇੰਟ 0 ਤੋਂ ਘੱਟ ਨਹੀਂ ਹੋ ਸਕਦੇ।");
        document.getElementById('working').value = '';
        document.getElementById('non_functional').value = '';
        document.getElementById('non_working').value = '';
        document.getElementById('under_repair').value = ''; 
        document.getElementById('new_complaints').value = ''; 
        return; // Exit the function if the condition is met
    }

    // Set the calculated value to the non_functional input field
    document.getElementById('non_functional').value = nonFunctional;
}
document.getElementById('under_repair').value = parseInt(document.getElementById('under_repair').value) || 0;
// Add event listeners to calculate values and validate input when any input changes
document.getElementById('working').addEventListener('input', calculateNonFunctional);
document.getElementById('non_working').addEventListener('input', calculateNonFunctional);
document.getElementById('under_repair').addEventListener('input', calculateNonFunctional);
document.getElementById('new_complaints').addEventListener('input', calculateNonFunctional);
</script>

                    <div class="form-group">
                      <label for="exampleInputEmail1">10.) Remarks (ਟਿੱਪਣੀਆਂ)</label>
                      <input id="remarks" name="remarks" style=' border: 1px solid #ccc;  border-radius: 20px; height:40px' maxlength="300" type="text" class="form-control" required="true" value="" >
                    </div>

                    </div>

                     <br>
                   
                     <?php 

$sql2 = "SELECT districtid, townid FROM streetlightlogin where id=".$_SESSION['aid'];
                       $results = $con->query($sql2);
                       //echo $sql2;
                       if ($results->num_rows > 0) {
                        
                         while ($row = mysqli_fetch_assoc($results)) {
                           $townid=$row['townid']; 
                         }
                       }





 if ($townid != '0') {
                        $query1 = "SELECT  DOA as datedata
          FROM streetlightdata 
          WHERE town_id = '".$townid."'
          AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='".$townid."'); ";
                      }

                      $results = $con->query($query1);
                      
                      if ($results->num_rows > 0) {
                          while ($row = $results->fetch_assoc()) {
                              $totsccount = $row['tottal'];
                              $datetime= $row['datedata'];
                              list($date, $time) = explode(' ', $datetime);

                          }
                  
                      } 
              
       if (date("Y-m-d")>$date && ($value!='0' || $value!=0)) {



               echo'     <button type="submit" class="btn btn-primary mr-2"  name="sa">Submit</button>';
           }    ?>
         </form>
                  
  
                </div> </form>
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