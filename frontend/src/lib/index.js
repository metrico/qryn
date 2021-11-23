export function apiErrorToString (error) {
  return error?.response?.data?.message ||
    error?.response?.data?.error ||
    error?.message ||
    error.toString()
}
