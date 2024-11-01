
document.querySelector('.first-tab').style.backgroundColor = '#00A0DD';
document.getElementById('bulk-transaction').style.display = 'flex';
document.getElementById('bulk-transaction').style.width = '100%';


const sidebarTabs = document.querySelectorAll('.sidebar-tab')

sidebarTabs.forEach(tab => {
    tab.addEventListener('click', (event) =>{
        const tabSelected = event.currentTarget.getAttribute('data-id');
        document.querySelectorAll('.sidebar-tab').forEach( tab => {
            tab.style.backgroundColor = '#54565B';
        });

        document.querySelectorAll('.content-selection').forEach(contentWindow => {
            contentWindow.style.display = 'none';
        })

        event.currentTarget.style.backgroundColor = '#00A0DD';
        document.getElementById(tabSelected).style.display = 'flex';
        document.getElementById(tabSelected).style.width = '100%';
    })
})

const uploadFile = document.getElementById('upload-file');
const fileInput = document.getElementById('file-input')
uploadFile.addEventListener('click', () => {
    fileInput.click();
});


fileInput.addEventListener('change', () => {
    const file = fileInput.files[0];
    if (file) {
        const formData = new FormData();
        formData.append('file', file);

        fetch('/upload-data', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            alert(data.message); // Display response message
        })
        .catch(error => {
            console.error('Error:', error);
        });
    }
});

