document.addEventListener('click', (event) => {
  const button = event.target.closest('[data-partner-toggle]');
  if (!button) return;
  const detail = document.getElementById(button.dataset.partnerToggle);
  if (!detail) return;
  detail.hidden = !detail.hidden;
  button.setAttribute('aria-expanded', String(!detail.hidden));
  button.textContent = detail.hidden ? 'Деталі' : 'Згорнути';
});
