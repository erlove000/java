
<?php session_start();
error_reporting(0);
// Database Connection
include('includes/config.php');
//Validating Session
if(strlen($_SESSION['aid'])==0)
  { 
header('location:login.php');
}
else{


  ?><?php $id=intval($_GET['id']);  ?><?php
require('includes/fpdf/fpdf.php');

// Database Connection
include('includes/config.php');


// Fetch data from the database
 $sql = "select s.*,d.district_name,t.town_name  from sbmu_release_data s, district d, sbmu_towns t where s.release_id='$id'
and d.district_id=s.release_dist
and t.town_id=s.release_town";
$result = $con->query($sql);
class PDF extends FPDF
{
    function Header()
    {
        $adImagePath = 'pmidc.jpg'; // Replace with the actual path to your ad image file
        $this->Image($adImagePath, 10, 10, 30, 0, 'JPG');
        // Set font and size
        $this->SetFont('Arial', 'B', 24);

        // Header text
        $this->Cell(0, 10, 'Swach Bharat Mission- PUNJAB', 0, 1, 'C');

        // Line break
        $this->Ln(10);
    }

    function Footer()
    {
        // Footer content
    }

    function Content($result)
    {
        // Set font and size
        $this->SetFont('Arial', '', 12);

        // Loop through the result set and display records
        while ($row = $result->fetch_assoc()) {
            $a=$row['release_id'];
            $c=$row['district_name'];
            $d=$row['town_name'];
            $e=$row['release_cat'];
            $f= $row['release_scheme'];
            $g=$row['release_seats'];
            $h=$row['release_date'];
        }
        $this->Cell(0, 10, 'Release Id: ' . $a, 0, 1);
        $this->Cell(0, 10, 'District Name: ' . $c, 0, 1);
        $this->Cell(0, 10, 'Town Name: ' . $d, 0, 1);
        $this->Cell(0, 10, 'Category of Release: ' . $e, 0, 1);
        $this->Cell(0, 10, 'Scheme: ' . $f, 0, 1);
        $this->Cell(0, 10, 'Released Seats: ' . $g, 0, 1);
        $this->Cell(0, 10, 'Release Date: ' . $h, 0, 1);
        
        $this->Ln(10);
        
    }
}

// Create PDF object
$pdf = new PDF();

// Set document properties
$pdf->SetTitle('User Data');

// Add a new page
$pdf->AddPage();

// Call the content method and pass the result set
$pdf->Content($result);

// Output PDF
$pdf->Output();

}?>
