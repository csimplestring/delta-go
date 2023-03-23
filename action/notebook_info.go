package action

type NotebookInfo struct {
	NotebookId string `json:"notebookId,omitempty"`
}

func NotebookInfoFromContext(context map[string]string) *NotebookInfo {
	if v, ok := context["notebookId"]; ok {
		return &NotebookInfo{
			NotebookId: v,
		}
	}
	return nil
}
