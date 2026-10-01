<?php session_start();
// Database Connection
include('includes/config.php');

// Validating Session
if (strlen($_SESSION['aid']) == 0) {
    header('location:index.php');
} else { ?>
<!DOCTYPE html>
<html lang="en">

<head>
    <title>Street Light Monitoring || Dashboard</title>
    <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
    <link rel="stylesheet" href="vendors/typicons/typicons.css">
    <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
    <link rel="stylesheet" href="css/vertical-layout-light/style.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels"></script> <!-- Data labels plugin -->
    <style>
        /* Additional CSS for better layout */
        body {
            height: auto;
            overflow-y: scroll; /* Ensure vertical scrolling */
        }

        .container-fluid {
            padding: 20px; /* Add padding to give space */
        }

        canvas {
            max-width: 100%; /* Ensure the canvas fits the container */
            height: 400px; /* Set a fixed height for the charts */
        }
    </style>
</head>

<body>
    <div class="container-scroller">
        <?php include_once('includes/header.php'); ?>
        <div class="container-fluid page-body-wrapper">
            <?php include_once('includes/sidebar.php'); ?>

            <div class="main-panel">
                <div class="content-wrapper">
                    <div class="row">
                        <div class="col-md-12 grid-margin stretch-card">
                            <div class="card">
                                <div class="card-body">
                                    <h4 class="card-title">Streetlight Data for Current Month</h4>

                                    <div>
                                        <canvas id="myChartWorking"></canvas>
                                    </div>
                                    <div>
                                        <canvas id="myChartNonWorking"></canvas>
                                    </div>
                                    <?php
                                    // Fetch current month data from streetlightdata table
                                    $townid = 0;
                                    $aid = $_SESSION['aid'];
                                    $sql2 = "SELECT townid FROM streetlightlogin WHERE id=$aid";
                                    $results = $con->query($sql2);
                                    if ($results->num_rows > 0) {
                                        while ($row = $results->fetch_assoc()) {
                                            $townid = $row['townid'];
                                        }
                                    }

                                    $query = "";
                                    if ($townid == '0') {
                                        $query = "SELECT DATE(DOA) as date,
                                                 SUM(total_working_street_light) AS working,
                                                 SUM(total_non_working_street_light) AS non_working
                                                 FROM streetlightdata
                                                 WHERE MONTH(DOA) = MONTH(CURRENT_DATE)
                                                 AND YEAR(DOA) = YEAR(CURRENT_DATE)
                                                 GROUP BY DATE(DOA)
                                                 ORDER BY DATE(DOA) ASC"; // Ascending order
                                    } else {
                                        $query = "SELECT DATE(DOA) as date,
                                                 total_working_street_light AS working,
                                                 total_non_working_street_light AS non_working
                                                 FROM streetlightdata
                                                 WHERE town_id = $townid
                                                 AND MONTH(DOA) = MONTH(CURRENT_DATE)
                                                 AND YEAR(DOA) = YEAR(CURRENT_DATE)
                                                 ORDER BY DATE(DOA) ASC"; // Ascending order
                                    }

                                    $results = $con->query($query);

                                    $dataWorking = [];
                                    $dataNonWorking = [];
                                    $labels = [];
                                    if ($results->num_rows > 0) {
                                        while ($row = $results->fetch_assoc()) {
                                            $labels[] = $row['date'];
                                            $dataWorking[] = $row['working'];
                                            $dataNonWorking[] = $row['non_working'];
                                        }
                                    }
                                    ?>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- Chart.js Line Chart Script -->
    <script>
        const ctxWorking = document.getElementById('myChartWorking').getContext('2d');
        const ctxNonWorking = document.getElementById('myChartNonWorking').getContext('2d');
        const labels = <?php echo json_encode($labels); ?>;
        const dataWorking = <?php echo json_encode($dataWorking); ?>;
        const dataNonWorking = <?php echo json_encode($dataNonWorking); ?>;

        const myChartWorking = new Chart(ctxWorking, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Total Working Streetlights',
                    data: dataWorking,
                    borderColor: 'rgba(75, 192, 192, 1)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    fill: true,
                    tension: 0.1,
                    datalabels: {
                        anchor: 'end',
                        align: 'end',
                        formatter: (value) => value // Display the data value
                    }
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    title: {
                        display: true,
                        text: 'Total Working Streetlights'
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return context.dataset.label + ': ' + context.raw;
                            }
                        }
                    },
                    datalabels: {
                        display: true, // Show data labels
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Date'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Count'
                        }
                    }
                }
            },
            plugins: [ChartDataLabels] // Use the data labels plugin
        });

        const myChartNonWorking = new Chart(ctxNonWorking, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Total Non-Working Streetlights',
                    data: dataNonWorking,
                    borderColor: 'rgba(255, 99, 132, 1)',
                    backgroundColor: 'rgba(255, 99, 132, 0.2)',
                    fill: true,
                    tension: 0.1,
                    datalabels: {
                        anchor: 'end',
                        align: 'end',
                        formatter: (value) => value // Display the data value
                    }
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    title: {
                        display: true,
                        text: 'Total Non-Working Streetlights'
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return context.dataset.label + ': ' + context.raw;
                            }
                        }
                    },
                    datalabels: {
                        display: true, // Show data labels
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Date'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Count'
                        }
                    }
                }
            },
            plugins: [ChartDataLabels] // Use the data labels plugin
        });
    </script>
    <script src="vendors/js/vendor.bundle.base.js"></script>
    <script src="js/off-canvas.js"></script>
    <script src="js/hoverable-collapse.js"></script>
    <script src="js/template.js"></script>
</body>

</html>
<?php } ?>
