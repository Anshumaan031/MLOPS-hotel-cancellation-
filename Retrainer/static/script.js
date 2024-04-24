document.addEventListener("DOMContentLoaded", function() {
    // This function runs when the document is loaded
    var predictButton = document.getElementById('predict-btn');
    
    predictButton.addEventListener('click', function() {
        console.log('The button has been clicked')
        fetch('/predict', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
        },
        // Assuming you don't need to send data to predict, otherwise include body
        // body: JSON.stringify({ yourDataKey: yourDataValue }),
        })
        .then(response => response.json())
        .then(data => {
        console.log('Success:', data);
        // Here you can also update the DOM with the prediction results if needed
        })
        .catch((error) => {
        console.error('Error:', error);
        });
    });
});
  