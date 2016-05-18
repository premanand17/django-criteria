from django import forms


class CriteriaForm(forms.Form):
    query = forms.CharField(widget=forms.Textarea)
