<?php 
session_start();
error_reporting(0);
error_reporting(E_ALL);

include('includes/config.php');
if (strlen($_SESSION['aid'] == 0)) {
    header('location:logout.php');
} else {
    $sql2 = "SELECT districtid, townid, staff_name FROM streetlightlogin WHERE id=" . $_SESSION['aid'];
    $results = $con->query($sql2);
    
    if ($results->num_rows > 0) {
        while ($row = mysqli_fetch_assoc($results)) {
            $districtid = $row['districtid'];
            $townid = $row['townid']; 
            $loggername = $row['staff_name'];
        }
    }  

    $total_applications_display = null; // Variable to hold the total applications to display
    $previous_total_applications = null; // Variable to hold the previous total applications
    $today_date = date('Y-m-d'); // Get current date in Y-m-d format

    if (isset($_POST['sa'])) {
        $distid = isset($_POST['district']) ? $_POST['district'] : null;
        $town_id = isset($_POST['town_id']) ? $_POST['town_id'] : null;
        $dob = isset($_POST['dob']) ? $_POST['dob'] : null;
        $name_of_service = isset($_POST['name_of_service']) ? $_POST['name_of_service'] : null;
        $days_required = isset($_POST['days_required']) ? $_POST['days_required'] : null;
        $total_applications = isset($_POST['total_applications']) ? $_POST['total_applications'] : null;
        $pending_beyond_timelines = isset($_POST['pending_beyond_timelines']) ? $_POST['pending_beyond_timelines'] : null;

        // Validate date of reporting
        if ($dob > $today_date) {
            echo '<script>
                alert("You cannot enter data for a future date.");
                window.location.href = "PBTRAC.php";
            </script>';
            exit();
        }

        // Check if data for same date and same service already exists
        $query1 = "SELECT DATE(date_of_reporting) AS datedata, total_applications FROM pbtrac_report WHERE ulb = '$town_id' AND date_of_reporting ='" . $dob . " 00:00:00' AND name_of_service = '$name_of_service'";
        $results = $con->query($query1);
        
        $istrue = false;

        if ($results && $results->num_rows > 0) {
            $istrue = true;
            echo '<script>
                alert("Data for the same date and service is already present.");
                window.location.href = "PBTRAC.php";
            </script>';
        }  

        // Fetch the last total_applications for the specific service for validation
        $query2 = "SELECT total_applications FROM pbtrac_report WHERE ulb = '$town_id' AND name_of_service = '$name_of_service' ORDER BY date_of_reporting DESC LIMIT 1";
        $lastResult = $con->query($query2);
        if ($lastResult && $lastResult->num_rows > 0) {
            $lastRow = $lastResult->fetch_assoc();
            $previous_total_applications = $lastRow['total_applications']; // Store the previous total applications
        }

        // Validation checks
        if ($istrue == false) {
            if ($previous_total_applications !== null && $total_applications < $previous_total_applications) {
                echo '<script>alert("Total Applications cannot be less than the previous total applications for this service.");</script>';
            } elseif ($pending_beyond_timelines > $total_applications) {
                echo '<script>alert("Pending Beyond Timelines cannot be greater than Total Applications.");</script>';
            } elseif ($pending_beyond_timelines < 0) {
                echo '<script>alert("Pending Beyond Timelines cannot be negative.");</script>';
            } else {
                $stmt = $con->prepare("INSERT INTO pbtrac_report 
                    (district, ulb, date_of_reporting, name_of_service, days_required, total_applications, pending_beyond_timelines) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)");

                if ($stmt === false) {
                    die("Prepare failed: " . $con->error);
                }

                $stmt->bind_param("sssssss", $distid, $town_id, $dob, $name_of_service, $days_required, $total_applications, $pending_beyond_timelines);

                if ($stmt->execute()) {
                    $total_applications_display = $total_applications; // Store the total applications for display
                    echo '<script>
                        alert("Data submitted successfully!");
                        window.location.href = "PBTRAC.php";
                    </script>';
                } else {
                    echo '<script>alert("Error: ' . $stmt->error . '");</script>';
                }
                $stmt->close();
            }
        }
    }
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <title>PBTRAC || Add Details</title>
    <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
    <link rel="stylesheet" href="vendors/typicons/typicons.css">
    <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
    <link rel="stylesheet" href="vendors/select2/select2.min.css">
    <link rel="stylesheet" href="vendors/select2-bootstrap-theme/select2-bootstrap.min.css">
    <link rel="stylesheet" href="css/vertical-layout-light/style.css">
    
    <script src="http://js.nicedit.com/nicEdit-latest.js" type="text/javascript"></script>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const serviceSelect = document.getElementById('name_of_service');
            const daysRequiredInput = document.getElementById('days_required');
            const totalApplicationsInput = document.getElementById('total_applications');
            const townId = document.getElementById('town_id');

            serviceSelect.addEventListener('change', function() {
                const selectedOption = this.options[this.selectedIndex];
                const daysRequired = selectedOption.getAttribute('data-days');
                daysRequiredInput.value = daysRequired || '0';

                // Fetch previous total applications
                const serviceName = selectedOption.value;
                const townIdValue = townId.value;

                if (serviceName && townIdValue) {
                    fetch(`get_previous_total.php?service=${encodeURIComponent(serviceName)}&town_id=${encodeURIComponent(townIdValue)}`)
                        .then(response => response.json())
                        .then(data => {
                            if (data.success) {
                                totalApplicationsInput.value = data.total_applications || '';
                            } else {
                                totalApplicationsInput.value = '';
                            }
                        })
                        .catch(error => console.error('Error fetching total applications:', error));
                }
            });
        });
    </script>
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
                                    <h4 class="card-title">Add PBTRAC Details</h4>
                                    
                                    <form class="forms-sample" method="POST" id="myForm" action="<?php echo $_SERVER['PHP_SELF']; ?>">  
                                        <div class="form-group">
                                            <label for="exampleInputUsername1">District Name (ਜ਼ਿਲ੍ਹੇ ਦਾ ਨਾਮ)</label>
                                            <?php
                                            if ($townid == '0') {
                                                $query = "SELECT district_name, distid FROM towns_streetlight_mapping GROUP BY district_name, distid ";
                                            } else {
                                                $query = "SELECT district_name, distid FROM towns_streetlight_mapping WHERE town_id = " . $townid;
                                            }
                                            $result = mysqli_query($con, $query);

                                            if (mysqli_num_rows($result) > 0) {
                                                echo '<select name="district" id="district" class="form-control" required="true" style="border: 1px solid #ccc; border-radius: 20px; height: 40px;">';
                                                echo "<option value=''>Select District From Below</option>";
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
                                                echo '<select id="town_id" name="town_id" class="form-control" required="true" style="border: 1px solid #ccc; border-radius: 20px; height: 40px;">';
                                                echo "<option value=''>Select ULB From Below</option>";

                                                while ($row = mysqli_fetch_assoc($result)) {
                                                    $town = $row['town_name'];
                                                    $town_id = $row['town_id'];
                                                    echo "<option value='$town_id'>$town</option>";
                                                }

                                                echo '</select>';
                                            }
                                            
                                            ?>
                                        </div>

                                        <div class="form-group">
                                            <label for="exampleInputEmail1">3.) Date of Reporting (ਰਿਪੋਰਟਿੰਗ ਦੀ ਮਿਤੀ)</label>
                                            <input id="dob" name="dob" style='border: 1px solid #ccc; border-radius: 20px; height:40px' type="date" class="form-control" required="true" value="<?php echo date('Y-m-d'); ?>" max="<?php echo date('Y-m-d'); ?>">
                                        </div>

                                        <div class="form-group">
                                            <label for="name_of_service">4.) Name of Service</label>
                                            <select name="name_of_service" id="name_of_service" class="form-control" required style="border: 1px solid #ccc; border-radius: 20px; height:40px">
                                                <option value="" disabled selected>Select a service</option>
                                                <option value="Approval for time extension for building plans" data-days="17">Approval for time extension for building plans</option>
                                                <option value="Approval of Sewerage Disconnection / Reconnection" data-days="9">Approval of Sewerage Disconnection / Reconnection</option>
                                                <option value="Approval of Water Disconnection/ Reconnection" data-days="7">Approval of Water Disconnection/ Reconnection</option>
                                                <option value="Issuance of Allotment Letters" data-days="62">Issuance of Allotment Letters</option>
                                                <option value="Issuance of Possession Letters" data-days="32">Issuance of Possession Letters</option>
                                                <option value="Issue of Conveyance Deed in Municipal Committees and Municipal Corporations" data-days="17">Issue of Conveyance Deed in Municipal Committees and Municipal Corporations</option>
                                                <option value="Issue of Bus Pass (for buses operated by the ULB)" data-days="0">Issue of Bus Pass (for buses operated by the ULB)</option>
                                                <option value="Issue of Conveyance Deed" data-days="17">Issue of Conveyance Deed</option>
                                                <option value="Issue of No Due Certificate" data-days="9">Issue of No Due Certificate</option>
                                                <option value="Issue of No Objection Certificate/Duplicate Allotment/Re-allotment Letter" data-days="23">Issue of No Objection Certificate/Duplicate Allotment/Re-allotment Letter</option>
                                                <option value="Issue of permission for mortgage" data-days="9">Issue of permission for mortgage</option>
                                                <option value="License for Slaughter house" data-days="32">License for Slaughter house</option>
                                                <option value="Granting Road cutting Permission including checking of site" data-days="0">Granting Road cutting Permission including checking of site</option> 
                                                <option value="Verifications of proper road restoration" data-days="0">Verifications of proper road restoration</option>   
                                                <option value="Approval of Additional Construction" data-days="32">Approval of Additional Construction</option>   
                                                <option value="Change of Title in Water & Sewerage Bill Water & Sewerage Bill Amendment" data-days="9">Change of Title in Water & Sewerage Bill Water & Sewerage Bill Amendment</option> 
                                                <option value="Transfer of property in case of death (uncontested)" data-days="47">Transfer of property in case of death (uncontested)(Improvement trust)</option>
                                                <option value="Transfer of property in case of sale" data-days="17">Transfer of property in case of sale(Improvement trust)</option>                                                  
                                                
                                            </select>
                                        </div>

                                        <div class="form-group">
                                            <label for="days_required">5.) Number of days required as per PBTRAC</label>
                                            <input type="number" name="days_required" id="days_required" class="form-control" required readonly style="border: 1px solid #ccc; border-radius: 20px; height:40px">
                                        </div>

                                        <div class="form-group">
                                            <label for="total_applications">6.) Total Applications</label>
                                            <input type="number" name="total_applications" id="total_applications" class="form-control" required style="border: 1px solid #ccc; border-radius: 20px; height:40px" value="<?php echo $previous_total_applications !== null ? $previous_total_applications : ''; ?>">
                                        </div>

                                        <div class="form-group">
                                            <label for="pending_beyond_timelines">7.) No of application Pending Beyond Timelines</label>
                                            <input type="number" name="pending_beyond_timelines" id="pending_beyond_timelines" class="form-control" required style="border: 1px solid #ccc; border-radius: 20px; height:40px">
                                        </div>

                                        <button type="submit" class="btn btn-primary mr-2" name="sa">Submit</button>
                                    </form>

                                    <?php if ($total_applications_display !== null): ?>
                                        <div class="alert alert-info mt-3">
                                            <strong>Total Applications Entered:</strong> <?php echo $total_applications_display; ?>
                                        </div>
                                    <?php endif; ?>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <?php include_once('includes/footer.php');?>
            </div>
        </div>
    </div>

    <!-- base:js -->
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
    <!-- End custom js for this page-->
</body>
</html>
<?php } ?>


<?php
// get_previous_total.php
include('includes/config.php');

if (isset($_GET['service']) && isset($_GET['town_id'])) {
    $service = $_GET['service'];
    $town_id = $_GET['town_id'];

    $query = "SELECT total_applications FROM pbtrac_report WHERE ulb = '$town_id' AND name_of_service = '$service' ORDER BY date_of_reporting DESC LIMIT 1";
    $result = $con->query($query);

    if ($result && $result->num_rows > 0) {
        $row = $result->fetch_assoc();
        echo json_encode(['success' => true, 'total_applications' => $row['total_applications']]);
    } else {
        echo json_encode(['success' => true, 'total_applications' => null]);
    }
} else {
    echo json_encode(['success' => false]);
}
?>