# forms.py
from django import forms
from .models import ContactForm

class ContactFormModel(forms.ModelForm):
    class Meta:
        model = ContactForm
        fields = ['name', 'email', 'company_name', 'message']
