<?php 
session_start();
include_once('includes/config.php');

// Initialize variables
$from_date = "";
$to_date = "";
$details = [];

// Handle search request
if (isset($_POST['search'])) {
    $from_date = $_POST['from_date'];
    $to_date = $_POST['to_date'];
    $to_date = date('Y-m-d', strtotime($to_date . ' +1 day'));
    $sql2 = "SELECT districtid, townid FROM streetlightlogin WHERE id=".$_SESSION['aid'];
    $results = $con->query($sql2);
    if ($results->num_rows > 0) {
        while ($row = mysqli_fetch_assoc($results)) {
            $districtid = $row['districtid'];
            $townid = $row['townid'];
        }
    }
    // Fetch details based on the date range
    $sql = "SELECT * FROM streetlight_reason WHERE town_id='".$townid ."' and distid='".$districtid."'  AND DOA >= ? 
AND DOA <= ? ";

    $stmt = $con->prepare($sql);
    $stmt->bind_param('ss', $from_date, $to_date);
    $stmt->execute();
    $result = $stmt->get_result();
    
    if ($result->num_rows > 0) {
        $details = $result->fetch_all(MYSQLI_ASSOC);
    } else {
       // echo "<script>alert('No details found for the given date range.');</script>";
    }
}
?>

<!DOCTYPE html>
<html lang="en">
<head>
    <title>Streetlight || Search Details</title>
    <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
    <link rel="stylesheet" href="vendors/typicons/typicons.css">
    <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
    <link rel="stylesheet" href="vendors/select2/select2.min.css">
    <link rel="stylesheet" href="vendors/select2-bootstrap-theme/select2-bootstrap.min.css">
    <link rel="stylesheet" href="css/vertical-layout-light/style.css">
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
                                    <h4 class="card-title">Search Added Reason Details</h4>
                                    <form class="forms-sample" method="POST" action="<?php echo $_SERVER['PHP_SELF']; ?>">  
                                        <div class="form-group">
                                            <label for="from_date">From Date</label>
                                            <input id="from_date" name="from_date" type="date" class="form-control" required="true">
                                        </div>
                                        
                                        <div class="form-group">
                                            <label for="to_date">To Date</label>
                                            <input id="to_date" name="to_date" type="date" class="form-control" required="true">
                                        </div>

                                        <button type="submit" class="btn btn-primary mr-2" name="search">Search</button>
                                    </form>

                                    <?php if (!empty($details)): ?>
                                    <div class="mt-4">
                                        <h5>Details for: <?php echo htmlspecialchars($from_date); ?> to <?php echo htmlspecialchars($to_date); ?></h5>
                                        <ul>
                                            <?php foreach ($details as $detail): ?>
                                            <li>
                                                <strong>District Name:</strong> <?php echo htmlspecialchars($detail['district_name']); ?><br>
                                                <!-- <strong>Town ID:</strong> <?php echo htmlspecialchars($detail['town_id']); ?><br> -->
                                                <strong>Date:</strong> <?php echo htmlspecialchars($detail['date']); ?><br>
                                                <strong>Location:</strong> <?php echo htmlspecialchars($detail['location']); ?><br>
                                                <strong>Description:</strong> <?php echo htmlspecialchars($detail['Description']); ?><br>
                                                <strong>Action to be Taken:</strong> <?php echo htmlspecialchars($detail['action_to_be_taken']); ?><br>
                                                <hr>
                                            </li>
                                            <?php endforeach; ?>
                                        </ul>
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

    <!-- JavaScript -->
    <script src="vendors/js/vendor.bundle.base.js"></script>
</body>
</html>
