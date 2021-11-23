export function getFormikError ({ errors, touched, inputName }) {
  return errors[inputName] && touched[inputName] && errors[inputName]
}
