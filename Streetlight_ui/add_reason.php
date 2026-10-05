<?php session_start();
error_reporting(E_ALL);
// ini_set('display_errors', 1);
include_once('includes/config.php');

// Check if user is logged in
if (!isset($_SESSION['aid']) || strlen($_SESSION['aid']) == 0) {
    header('location:logout.php');
    exit();
}
if (isset($_POST['sa'])) {
    echo 'Form is submitted!'; // Debug line

  
    // Proceed to retrieve the values
    $dob = isset($_POST['dob']) ? $_POST['dob'] : null;
    $district = isset($_POST['district']) ? $_POST['district'] : null;
    $location = isset($_POST['location']) ? $_POST['location'] : null;
    $Description = isset($_POST['Description']) ? $_POST['Description'] : null;
    $Action = isset($_POST['Action']) ? $_POST['Action'] : null;
    $distid = isset($_POST['distid']) ? $_POST['distid'] : null;
    $town_name = isset($_POST['town_name']) ? $_POST['town_name'] : null;
    $town_id = isset($_POST['town_id']) ? $_POST['town_id'] : null;
//     echo "<script>
//     alert('Values submitted:\\nDOB: $dob\\nDistrict: $district\\nLocation: $location\\nDescription: $Description\\nAction: $Action\\nDistrict ID: $distid\\nTown Name: $town_name\\nTown ID: $town_id');
// </script>";
    // Input validation
    if (empty($dob)) {
        echo '<script>alert("DOB is missing");</script>';
    } elseif (empty($Action)) {
        echo '<script>alert("Action is missing");</script>';
    } else {
        // Using prepared statements to handle SQL query
        $stmt = $con->prepare("INSERT INTO streetlight_reason (district_name, town_id, town_name, distid, date, location, Description, action_to_be_taken) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        if ($stmt === false) {
            echo "<script>alert('Failed to prepare the SQL statement: " . htmlspecialchars($con->error) . "');</script>";
            exit();
        }

        $stmt->bind_param("ssssssss", $district, $town_id,$town_name,$distid,   $dob, $location, $Description, $Action);

        if (!$stmt->execute()) {
            echo "<script>alert('Something went wrong: " . htmlspecialchars($stmt->error) . "');</script>";
        } else {
            echo "<script>alert('Details submitted successfully');</script>";
        }
        $stmt->close();
    }
}

?>

<!DOCTYPE html>
<html lang="en">
<head>
    <title>Streetlight || Add Details</title>
    <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
    <!-- CSS -->
    <link rel="stylesheet" href="vendors/typicons/typicons.css">
    <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
    <link rel="stylesheet" href="vendors/select2/select2.min.css">
    <link rel="stylesheet" href="vendors/select2-bootstrap-theme/select2-bootstrap.min.css">
    <link rel="stylesheet" href="css/vertical-layout-light/style.css">
    <script src="http://js.nicedit.com/nicEdit-latest.js" type="text/javascript"></script>
    <script type="text/javascript">bkLib.onDomLoaded(nicEditors.allTextAreas);</script>
</head>

<body>
    <div class="container-scroller">
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

        <div class="container-fluid page-body-wrapper">
            <?php include_once('includes/sidebar.php');?>

            <div class="main-panel">        
                <div class="content-wrapper">
                    <div class="row">
                        <div class="col-md-12 grid-margin stretch-card">
                            <div class="card">
                                <div class="card-body">
                                    <h4 class="card-title">Add Reason For Streetlight</h4>
                                    <form class="forms-sample" method="POST" id="myForm" action="<?php echo $_SERVER['PHP_SELF']; ?>">
                                    <div class="form-group">
                                            <label for="district">Select District Name From The List</label>
                                            <?php
                                            $sql2 = "SELECT districtid, townid FROM streetlightlogin WHERE id=".$_SESSION['aid'];
                                            $results = $con->query($sql2);
                                            if ($results->num_rows > 0) {
                                                while ($row = mysqli_fetch_assoc($results)) {
                                                    $districtid = $row['districtid'];
                                                    $townid = $row['townid'];
                                                }
                                            }
                                            $sql = "SELECT * FROM towns_streetlight_mapping WHERE distid='".$districtid."' limit 1";
                                         //   echo $sql;
                                            $result = $con->query($sql);
                                            if ($result->num_rows > 0) {
                                                while ($row = mysqli_fetch_assoc($result)) {
                                                    $optionValue = $row['district_name'];
                                                    $optionValue1 = $row['distid'];
                                                    echo "<input type='text' class='form-control' name='district' id='district' value='$optionValue' readonly style='border: 1px solid #ccc; border-radius: 20px; height:40px'>";
                                                    echo "<input type='hidden' class='form-control' name='distid' id='distid' value='$optionValue1' readonly style='border: 1px solid #ccc; border-radius: 20px; height:40px'>";
                                                }
                                            }
                                            ?>
                                        </div>


                                        <div class="form-group">
                                            <label for="district">Select Town Name From The List</label>
                                            <?php
                                            $sql2 = "SELECT districtid FROM streetlightlogin WHERE id=".$_SESSION['aid'];
                                            $results = $con->query($sql2);
                                            if ($results->num_rows > 0) {
                                                while ($row = mysqli_fetch_assoc($results)) {
                                                    $districtid = $row['districtid'];
                                                }
                                            }
                                            $sql = "SELECT * FROM towns_streetlight_mapping WHERE town_id='".$townid."' limit 1";
                                            $result = $con->query($sql);
                                            if ($result->num_rows > 0) {
                                                while ($row = mysqli_fetch_assoc($result)) {
                                                    $optionValue = $row['town_name'];
                                                    $optionValue1 = $row['town_id'];
                                                    echo "<input type='text' class='form-control' name='town_name' id='town_name' value='$optionValue' readonly style='border: 1px solid #ccc; border-radius: 20px; height:40px'>";
                                                    echo "<input type='hidden' class='form-control' name='town_id' id='town_id' value='$optionValue1' readonly style='border: 1px solid #ccc; border-radius: 20px; height:40px'>";
                                                }
                                            }
                                            ?>
                                        </div>





                                        <div class="form-group">
                                            <label for="dob">Date</label>
                                            <input id="dob" name="dob" type="date" class="form-control" required="true" min="2020-01-01" max="<?php echo date('Y-m-d'); ?>" style='border: 1px solid #ccc; border-radius: 20px; height:40px'>
                                        </div>

                                        <div class="form-group">
                                            <label for="location">Location of Streetlight</label>
                                            <input id="location" name="location" type="text" class="form-control" required="true" style='border: 1px solid #ccc; border-radius: 20px; height:40px'>
                                        </div>

                                        <div class="form-group">
                                            <label for="Description">Description</label>
                                            <input id="Description" name="Description" type="text" class="form-control" required="true" style='border: 1px solid #ccc; border-radius: 20px; height:40px'>
                                        </div>

                                        <div class="form-group">
                                            <label for="Action">Action to be taken</label>
                                            <input id="Action" name="Action" type="text" class="form-control" required="true" style='border: 1px solid #ccc; border-radius: 20px; height:40px'>
                                        </div>

                                        <button type="submit" class="btn btn-primary mr-2" name="sa">Submit</button>
                                        </form>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <?php include_once('includes/footer.php');?>
            </div>
        </div>
    </div>

    <!-- JavaScript -->
    <script src="vendors/js/vendor.bundle.base.js"></script>
    <script src="js/off-canvas.js"></script>
    <script src="js/hoverable-collapse.js"></script>
    <script src="js/template.js"></script>
    <script src="js/settings.js"></script>
    <script src="js/todolist.js"></script>
    <script src="vendors/typeahead.js/typeahead.bundle.min.js"></script>
    <script src="vendors/select2/select2.min.js"></script>
    <script src="js/file-upload.js"></script>
    <script src="js/typeahead.js"></script>
    <script src="js/select2.js"></script>
</body>
</html>
