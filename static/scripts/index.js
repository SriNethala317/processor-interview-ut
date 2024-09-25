const loginForm = document.getElementById("login-form");

loginForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const username = document.getElementById("username").value;
  const password = document.getElementById("password").value;
  const errMsg = document.getElementById("err-msg");

  if (!username || !password) {
    errMsg.textContent = "Invalid Username or Password";
  } else {
    const formData = new FormData(loginForm);
    fetch('/login', {
      method: 'POST',
      body: formData
    }).then(response => response.json())
    .then(data => {
      if(data.success){
        window.location.href = '/templates/dashboard.html'
      } else {
        errMsg.textContent = "Invalid Username or Password";
      }
    })
  }
});